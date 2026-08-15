package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
)

const (
	defaultSparkMaintenanceMainClass = "org.apache.spark.deploy.SparkSubmit"
	defaultSparkMaintenanceCatalog   = "rivus"
	maxMaintenanceTables             = 100
	maxMaintenanceOperations         = 10
	maxSparkResponseBytes            = 1 << 20
)

var (
	sparkCatalogNamePattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	sparkSubmissionIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

// SparkMaintenanceError identifies failures returned by, or communicating
// with, the configured maintenance execution service. The historical name is
// retained for API compatibility.
type SparkMaintenanceError struct {
	Err error
}

func (e *SparkMaintenanceError) Error() string {
	if e == nil || e.Err == nil {
		return "Spark maintenance request failed"
	}
	return e.Err.Error()
}

func (e *SparkMaintenanceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// TableMaintenanceRequest describes one Spark job. Each operation is run for
// every selected table in the order supplied.
type TableMaintenanceRequest struct {
	Tables         []string                    `json:"tables,omitempty"`
	Operations     []TableMaintenanceOperation `json:"operations"`
	ExternalRunKey string                      `json:"-"`
}

type TableMaintenanceOperation struct {
	Type    string         `json:"type"`
	Options map[string]any `json:"options,omitempty"`
}

type TableMaintenanceSubmission struct {
	Action             string   `json:"action,omitempty"`
	Message            string   `json:"message,omitempty"`
	ServerSparkVersion string   `json:"server_spark_version,omitempty"`
	SubmissionID       string   `json:"submission_id,omitempty"`
	Success            bool     `json:"success"`
	Tables             []string `json:"tables,omitempty"`
	Operations         []string `json:"operations,omitempty"`
}

type SparkSubmissionStatus struct {
	Action             string `json:"action,omitempty"`
	DriverState        string `json:"driver_state,omitempty"`
	Message            string `json:"message,omitempty"`
	ServerSparkVersion string `json:"server_spark_version,omitempty"`
	SubmissionID       string `json:"submission_id,omitempty"`
	Success            bool   `json:"success"`
	WorkerHostPort     string `json:"worker_host_port,omitempty"`
	WorkerID           string `json:"worker_id,omitempty"`
}

type sparkCreateSubmissionRequest struct {
	Action               string            `json:"action"`
	AppArgs              []string          `json:"appArgs"`
	AppResource          string            `json:"appResource"`
	ClientSparkVersion   string            `json:"clientSparkVersion"`
	EnvironmentVariables map[string]string `json:"environmentVariables"`
	MainClass            string            `json:"mainClass"`
	SparkProperties      map[string]string `json:"sparkProperties"`
}

type sparkSubmissionResponse struct {
	Action             string `json:"action"`
	DriverState        string `json:"driverState"`
	Message            string `json:"message"`
	ServerSparkVersion string `json:"serverSparkVersion"`
	SubmissionID       string `json:"submissionId"`
	Success            bool   `json:"success"`
	WorkerHostPort     string `json:"workerHostPort"`
	WorkerID           string `json:"workerId"`
}

type runnerMaintenanceRequest struct {
	Filename        string         `json:"filename"`
	Content         string         `json:"content"`
	SourceSystem    string         `json:"source_system"`
	JobName         string         `json:"job_name,omitempty"`
	RequestedBy     string         `json:"requested_by_type"`
	RequestedByRef  string         `json:"requested_by_ref,omitempty"`
	ExternalRunKey  string         `json:"external_run_key,omitempty"`
	Catalog         string         `json:"catalog,omitempty"`
	ResourceProfile string         `json:"resource_profile,omitempty"`
	JobContext      map[string]any `json:"job_context,omitempty"`
}

type runnerMaintenanceResponse struct {
	JobID   string `json:"job_id"`
	JobName string `json:"job_name"`
	Status  string `json:"status"`
	Error   string `json:"error"`
}

type maintenancePayload struct {
	JobID      string                 `json:"job_id"`
	Statements []maintenanceStatement `json:"statements"`
}

type maintenanceStatement struct {
	Operation string `json:"operation"`
	Table     string `json:"table"`
	SQL       string `json:"sql"`
}

var maintenanceOptionNames = map[string]map[string]struct{}{
	"rewrite_data_files":            optionSet("strategy", "sort_order", "options", "where"),
	"rewrite_manifests":             optionSet("use_caching", "spec_id", "sort_by"),
	"rewrite_position_delete_files": optionSet("options", "where"),
	"expire_snapshots": optionSet(
		"older_than", "retain_last", "max_concurrent_deletes", "stream_results",
		"snapshot_ids", "clean_expired_metadata",
	),
	"remove_orphan_files": optionSet(
		"older_than", "location", "dry_run", "max_concurrent_deletes", "stream_results",
		"file_list_view", "equal_schemes", "equal_authorities", "prefix_mismatch_mode",
		"prefix_listing",
	),
}

func optionSet(names ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(names))
	for _, name := range names {
		out[name] = struct{}{}
	}
	return out
}

// SubmitTableMaintenanceForJobConfig submits Iceberg maintenance through
// runner-app or the direct Spark fallback without pausing the Rivus stream.
func SubmitTableMaintenanceForJobConfig(
	ctx context.Context,
	jobID string,
	jobCfg *config.JobConfig,
	req TableMaintenanceRequest,
	streaming bool,
) (*TableMaintenanceSubmission, error) {
	iceCfg, targets, operations, statements, err := prepareTableMaintenance(jobCfg, req, streaming)
	if err != nil {
		return nil, err
	}
	return submitPreparedTableMaintenance(ctx, jobID, jobCfg.Name, iceCfg, targets, operations, statements, req.ExternalRunKey)
}

func submitPreparedTableMaintenance(
	ctx context.Context,
	jobID string,
	jobName string,
	iceCfg config.IcebergConfig,
	targets []config.IcebergTarget,
	operations []string,
	statements []maintenanceStatement,
	externalRunKey string,
) (*TableMaintenanceSubmission, error) {
	if maintenanceUsesRunner(iceCfg.TableMaintenance) {
		return submitRunnerTableMaintenance(ctx, jobID, jobName, iceCfg, targets, operations, statements, externalRunKey)
	}
	payload, err := json.Marshal(maintenancePayload{JobID: jobID, Statements: statements})
	if err != nil {
		return nil, fmt.Errorf("encode Spark maintenance payload: %w", err)
	}
	createReq, err := buildSparkCreateRequest(jobID, jobName, iceCfg, string(payload))
	if err != nil {
		return nil, err
	}

	var response sparkSubmissionResponse
	if err := doSparkMaintenanceRequest(ctx, iceCfg.TableMaintenance, http.MethodPost, "/v1/submissions/create", createReq, &response); err != nil {
		return nil, err
	}
	if !response.Success || strings.TrimSpace(response.SubmissionID) == "" {
		message := strings.TrimSpace(response.Message)
		if message == "" {
			message = "Spark rejected the maintenance submission"
		}
		return nil, &SparkMaintenanceError{Err: fmt.Errorf("%s", message)}
	}

	tableNames := make([]string, 0, len(targets))
	for _, target := range targets {
		tableNames = append(tableNames, tableKey(target.Namespace, target.Table))
	}
	return &TableMaintenanceSubmission{
		Action:             response.Action,
		Message:            response.Message,
		ServerSparkVersion: response.ServerSparkVersion,
		SubmissionID:       response.SubmissionID,
		Success:            response.Success,
		Tables:             tableNames,
		Operations:         operations,
	}, nil
}

func submitRunnerTableMaintenance(
	ctx context.Context,
	jobID string,
	jobName string,
	iceCfg config.IcebergConfig,
	targets []config.IcebergTarget,
	operations []string,
	statements []maintenanceStatement,
	externalRunKey string,
) (*TableMaintenanceSubmission, error) {
	tableNames := make([]string, 0, len(targets))
	for _, target := range targets {
		tableNames = append(tableNames, tableKey(target.Namespace, target.Table))
	}

	content := make([]string, 0, len(statements)+1)
	content = append(content, "-- Generated by Rivus Iceberg table maintenance.")
	for _, statement := range statements {
		content = append(content, strings.TrimSuffix(strings.TrimSpace(statement.SQL), ";")+";")
	}

	now := time.Now().UTC().UnixNano()
	externalRunKey = strings.TrimSpace(externalRunKey)
	if externalRunKey == "" {
		externalRunKey = fmt.Sprintf("rivus-maintenance:%s:%d", jobID, now)
	}
	mode := maintenanceDisplayMode(operations)
	request := runnerMaintenanceRequest{
		Filename:        fmt.Sprintf("rivus_iceberg_maintenance_%d.sql", now),
		Content:         strings.Join(content, "\n"),
		SourceSystem:    "rivus",
		JobName:         maintenanceAppName(jobID, jobName),
		RequestedBy:     "service",
		RequestedByRef:  jobID,
		ExternalRunKey:  externalRunKey,
		Catalog:         maintenanceCatalogName(iceCfg),
		ResourceProfile: iceCfg.TableMaintenance.RunnerResourceProfile,
		JobContext: map[string]any{
			"runner_kind":      "iceberg_maintenance",
			"maintenance_mode": mode,
			"lineage_enabled":  false,
			"iceberg_maintenance": map[string]any{
				"catalog":             maintenanceCatalogName(iceCfg),
				"mode":                mode,
				"tables":              tableNames,
				"operations":          operations,
				"pause_rivus_writers": false,
			},
		},
	}

	var response runnerMaintenanceResponse
	if err := doRunnerMaintenanceRequest(ctx, iceCfg.TableMaintenance, http.MethodPost, "/internal/system/jobs/sql", request, &response); err != nil {
		return nil, err
	}
	if strings.TrimSpace(response.JobID) == "" {
		return nil, &SparkMaintenanceError{Err: fmt.Errorf("runner-app accepted maintenance without returning a job id")}
	}

	return &TableMaintenanceSubmission{
		Action:       "RunnerJobSubmission",
		Message:      response.Status,
		SubmissionID: response.JobID,
		Success:      true,
		Tables:       tableNames,
		Operations:   operations,
	}, nil
}

func maintenanceDisplayMode(operations []string) string {
	for _, operation := range operations {
		switch normalizeMaintenanceOperation(operation) {
		case "rewrite_data_files", "rewrite_manifests", "rewrite_position_delete_files":
			return "compaction"
		}
	}
	return "cleanup"
}

func GetTableMaintenanceStatusForJobConfig(ctx context.Context, jobCfg *config.JobConfig, submissionID string) (*SparkSubmissionStatus, error) {
	iceCfg, err := icebergConfigForMaintenance(jobCfg)
	if err != nil {
		return nil, err
	}
	if err := validateSparkSubmissionID(submissionID); err != nil {
		return nil, err
	}

	return getTableMaintenanceStatus(ctx, iceCfg, submissionID)
}

func CancelTableMaintenanceForJobConfig(ctx context.Context, jobCfg *config.JobConfig, submissionID string) (*SparkSubmissionStatus, error) {
	iceCfg, err := icebergConfigForMaintenance(jobCfg)
	if err != nil {
		return nil, err
	}
	if err := validateSparkSubmissionID(submissionID); err != nil {
		return nil, err
	}

	if maintenanceUsesRunner(iceCfg.TableMaintenance) {
		var response runnerMaintenanceResponse
		if err := doRunnerMaintenanceRequest(ctx, iceCfg.TableMaintenance, http.MethodPost, "/internal/system/jobs/"+submissionID+"/cancel", map[string]any{}, &response); err != nil {
			return nil, err
		}
		return runnerStatusResponse(response), nil
	}

	var response sparkSubmissionResponse
	if err := doSparkMaintenanceRequest(ctx, iceCfg.TableMaintenance, http.MethodPost, "/v1/submissions/kill/"+submissionID, map[string]any{}, &response); err != nil {
		return nil, err
	}
	return sparkStatusResponse(response), nil
}

func getTableMaintenanceStatus(ctx context.Context, iceCfg config.IcebergConfig, submissionID string) (*SparkSubmissionStatus, error) {
	if err := validateSparkSubmissionID(submissionID); err != nil {
		return nil, err
	}
	if maintenanceUsesRunner(iceCfg.TableMaintenance) {
		var response runnerMaintenanceResponse
		if err := doRunnerMaintenanceRequest(ctx, iceCfg.TableMaintenance, http.MethodGet, "/internal/system/jobs/"+submissionID, nil, &response); err != nil {
			return nil, err
		}
		return runnerStatusResponse(response), nil
	}

	var response sparkSubmissionResponse
	if err := doSparkMaintenanceRequest(ctx, iceCfg.TableMaintenance, http.MethodGet, "/v1/submissions/status/"+submissionID, nil, &response); err != nil {
		return nil, err
	}
	return sparkStatusResponse(response), nil
}

func runnerStatusResponse(response runnerMaintenanceResponse) *SparkSubmissionStatus {
	state := strings.ToUpper(strings.TrimSpace(response.Status))
	switch state {
	case "STARTING", "PENDING", "QUEUED":
		state = "SUBMITTED"
	case "RUNNING":
		state = "RUNNING"
	case "FINISHED", "SUCCEEDED", "SUCCESS":
		state = "FINISHED"
	case "CANCELLED", "CANCELED":
		state = "KILLED"
	case "FAILED":
		state = "FAILED"
	}
	message := strings.TrimSpace(response.Error)
	if message == "" {
		message = strings.TrimSpace(response.Status)
	}
	return &SparkSubmissionStatus{
		Action:       "RunnerJobStatus",
		DriverState:  state,
		Message:      message,
		SubmissionID: response.JobID,
		Success:      true,
	}
}

func sparkStatusResponse(response sparkSubmissionResponse) *SparkSubmissionStatus {
	return &SparkSubmissionStatus{
		Action:             response.Action,
		DriverState:        response.DriverState,
		Message:            response.Message,
		ServerSparkVersion: response.ServerSparkVersion,
		SubmissionID:       response.SubmissionID,
		Success:            response.Success,
		WorkerHostPort:     response.WorkerHostPort,
		WorkerID:           response.WorkerID,
	}
}

func prepareTableMaintenance(
	jobCfg *config.JobConfig,
	req TableMaintenanceRequest,
	streaming bool,
) (config.IcebergConfig, []config.IcebergTarget, []string, []maintenanceStatement, error) {
	iceCfg, err := icebergConfigForMaintenance(jobCfg)
	if err != nil {
		return config.IcebergConfig{}, nil, nil, nil, err
	}
	if len(req.Operations) == 0 {
		return config.IcebergConfig{}, nil, nil, nil, fmt.Errorf("at least one maintenance operation is required")
	}
	if len(req.Operations) > maxMaintenanceOperations {
		return config.IcebergConfig{}, nil, nil, nil, fmt.Errorf("maintenance request has %d operations; maximum is %d", len(req.Operations), maxMaintenanceOperations)
	}

	sink := &Sink{cfg: iceCfg}
	targets, err := orphanCleanupTargets(jobCfg, sink, req.Tables)
	if err != nil {
		return config.IcebergConfig{}, nil, nil, nil, err
	}
	if len(targets) == 0 {
		return config.IcebergConfig{}, nil, nil, nil, fmt.Errorf("no iceberg target tables found; pass explicit tables as namespace.table")
	}
	if len(targets) > maxMaintenanceTables {
		return config.IcebergConfig{}, nil, nil, nil, fmt.Errorf("maintenance request selects %d tables; maximum is %d", len(targets), maxMaintenanceTables)
	}

	catalogName := maintenanceCatalogName(iceCfg)
	operations := make([]string, 0, len(req.Operations))
	statements := make([]maintenanceStatement, 0, len(req.Operations)*len(targets))
	for _, operation := range req.Operations {
		operationType := normalizeMaintenanceOperation(operation.Type)
		if _, ok := maintenanceOptionNames[operationType]; !ok {
			return config.IcebergConfig{}, nil, nil, nil, fmt.Errorf("unsupported maintenance operation %q", operation.Type)
		}
		if streaming {
			if err := validateStreamingMaintenanceSafety(operationType, operation.Options, time.Now()); err != nil {
				return config.IcebergConfig{}, nil, nil, nil, err
			}
		}
		operations = append(operations, operationType)
		for _, target := range targets {
			sql, err := buildMaintenanceSQL(catalogName, target, operationType, operation.Options)
			if err != nil {
				return config.IcebergConfig{}, nil, nil, nil, err
			}
			statements = append(statements, maintenanceStatement{
				Operation: operationType,
				Table:     tableKey(target.Namespace, target.Table),
				SQL:       sql,
			})
		}
	}
	return iceCfg, targets, operations, statements, nil
}

func icebergConfigForMaintenance(jobCfg *config.JobConfig) (config.IcebergConfig, error) {
	if jobCfg == nil {
		return config.IcebergConfig{}, fmt.Errorf("job config is nil")
	}
	sinkType, sinkCfg := jobSinkSpec(jobCfg)
	if !strings.EqualFold(sinkType, "iceberg_native") {
		return config.IcebergConfig{}, fmt.Errorf("job sink is %q, not iceberg_native", sinkType)
	}
	iceCfg, err := decodeIcebergConfig(sinkCfg)
	if err != nil {
		return config.IcebergConfig{}, err
	}
	if err := validateMaintenanceBackend(iceCfg.TableMaintenance); err != nil {
		return config.IcebergConfig{}, err
	}
	if catalogName := maintenanceCatalogName(iceCfg); !sparkCatalogNamePattern.MatchString(catalogName) {
		return config.IcebergConfig{}, fmt.Errorf("invalid Spark Iceberg catalog name %q", catalogName)
	}
	return iceCfg, nil
}

func validateMaintenanceBackend(cfg config.IcebergTableMaintenanceConfig) error {
	if maintenanceUsesRunner(cfg) {
		if _, err := runnerBaseURL(maintenanceRunnerURI(cfg)); err != nil {
			return err
		}
		if maintenanceRunnerToken(cfg) == "" {
			return fmt.Errorf("iceberg table_maintenance.runner_api_token or RUNNER_API_TOKEN is required")
		}
		profile := strings.ToLower(strings.TrimSpace(cfg.RunnerResourceProfile))
		if profile == "" {
			profile = "small"
		}
		switch profile {
		case "tiny", "small", "medium", "large":
			return nil
		default:
			return fmt.Errorf("invalid iceberg table_maintenance.runner_resource_profile %q", profile)
		}
	}
	if strings.TrimSpace(cfg.SparkRESTURI) == "" {
		return fmt.Errorf("iceberg table_maintenance.runner_uri or spark_rest_uri is required")
	}
	if strings.TrimSpace(cfg.SparkMaster) == "" {
		return fmt.Errorf("iceberg table_maintenance.spark_master is required for Spark REST submission")
	}
	if strings.TrimSpace(cfg.AppResource) == "" {
		return fmt.Errorf("iceberg table_maintenance.app_resource is required for Spark REST submission")
	}
	_, err := sparkRESTBaseURL(cfg.SparkRESTURI)
	return err
}

func buildSparkCreateRequest(jobID, jobName string, iceCfg config.IcebergConfig, payload string) (sparkCreateSubmissionRequest, error) {
	maintenanceCfg := iceCfg.TableMaintenance
	properties, err := sparkMaintenanceProperties(jobID, jobName, iceCfg)
	if err != nil {
		return sparkCreateSubmissionRequest{}, err
	}
	mainClass := strings.TrimSpace(maintenanceCfg.MainClass)
	if mainClass == "" {
		mainClass = defaultSparkMaintenanceMainClass
	}
	appResource := strings.TrimSpace(maintenanceCfg.AppResource)
	sparkAppResource := appResource
	appArgs := make([]string, 0, 3)
	if mainClass == defaultSparkMaintenanceMainClass {
		// PySpark submissions use SparkSubmit as their main class and expect the
		// Python resource as the first application argument. Spark's REST
		// protocol leaves appResource empty for this form.
		sparkAppResource = ""
		appArgs = append(appArgs, appResource)
	}
	appArgs = append(appArgs, "--payload-json", payload)

	environment := make(map[string]string, len(maintenanceCfg.EnvironmentVariables))
	for key, value := range maintenanceCfg.EnvironmentVariables {
		if strings.TrimSpace(key) != "" {
			environment[key] = value
		}
	}
	return sparkCreateSubmissionRequest{
		Action:               "CreateSubmissionRequest",
		AppArgs:              appArgs,
		AppResource:          sparkAppResource,
		ClientSparkVersion:   strings.TrimSpace(maintenanceCfg.ClientSparkVersion),
		EnvironmentVariables: environment,
		MainClass:            mainClass,
		SparkProperties:      properties,
	}, nil
}

func sparkMaintenanceProperties(jobID, jobName string, iceCfg config.IcebergConfig) (map[string]string, error) {
	maintenanceCfg := iceCfg.TableMaintenance
	catalogName := maintenanceCatalogName(iceCfg)
	prefix := "spark.sql.catalog." + catalogName
	warehouse, err := catalogWarehouse(iceCfg)
	if err != nil {
		return nil, err
	}
	properties := map[string]string{
		"spark.master":            strings.TrimSpace(maintenanceCfg.SparkMaster),
		"spark.submit.deployMode": "cluster",
		"spark.app.name":          maintenanceAppName(jobID, jobName),
		"spark.sql.extensions":    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
		prefix:                    "org.apache.iceberg.spark.SparkCatalog",
		prefix + ".type":          "rest",
		prefix + ".uri":           catalogRESTURI(iceCfg),
		prefix + ".warehouse":     warehouse,
	}
	setProperty(properties, prefix+".credential", firstNonEmpty(iceCfg.Credential, os.Getenv("ICEBERG_CREDENTIAL"), os.Getenv("GRAVITINO_CREDENTIAL")))
	setProperty(properties, prefix+".token", firstNonEmpty(iceCfg.OAuthToken, os.Getenv("ICEBERG_OAUTH_TOKEN")))
	setProperty(properties, prefix+".oauth2-server-uri", firstNonEmpty(iceCfg.OAuthTokenURI, os.Getenv("ICEBERG_OAUTH_TOKEN_URI"), os.Getenv("ICEBERG_REST_AUTH_URI"), os.Getenv("GRAVITINO_OAUTH_TOKEN_URI")))
	setProperty(properties, prefix+".scope", firstNonEmpty(iceCfg.Scope, os.Getenv("ICEBERG_SCOPE"), os.Getenv("GRAVITINO_SCOPE")))
	setProperty(properties, prefix+".prefix", iceCfg.Prefix)

	if header := firstNonEmpty(iceCfg.RESTAuthHeader, os.Getenv("ICEBERG_REST_AUTH_HEADER")); header != "" {
		setProperty(properties, prefix+".header.Authorization", header)
	} else if username := firstNonEmpty(iceCfg.RESTBasicUsername, os.Getenv("ICEBERG_REST_BASIC_USERNAME"), os.Getenv("GRAVITINO_SIMPLE_AUTH_USER")); username != "" {
		properties[prefix+".rest.auth.type"] = "basic"
		properties[prefix+".rest.auth.basic.username"] = username
		properties[prefix+".rest.auth.basic.password"] = firstNonEmpty(iceCfg.RESTBasicPassword, os.Getenv("ICEBERG_REST_BASIC_PASSWORD"), os.Getenv("GRAVITINO_SIMPLE_AUTH_PASSWORD"))
	}

	endpoint := firstNonEmpty(iceCfg.S3Endpoint, os.Getenv("ICEBERG_S3_ENDPOINT"), os.Getenv("AWS_S3_ENDPOINT"), os.Getenv("AWS_ENDPOINT_URL_S3"))
	accessKey := firstNonEmpty(os.Getenv("ICEBERG_S3_ACCESS_KEY_ID"), os.Getenv("AWS_ACCESS_KEY_ID"))
	secretKey := firstNonEmpty(os.Getenv("ICEBERG_S3_SECRET_ACCESS_KEY"), os.Getenv("AWS_SECRET_ACCESS_KEY"))
	if endpoint != "" || accessKey != "" || secretKey != "" {
		properties[prefix+".io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
	}
	setProperty(properties, prefix+".s3.endpoint", endpoint)
	setProperty(properties, prefix+".s3.region", firstNonEmpty(iceCfg.S3Region, os.Getenv("ICEBERG_S3_REGION"), os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION")))
	setProperty(properties, prefix+".s3.access-key-id", accessKey)
	setProperty(properties, prefix+".s3.secret-access-key", secretKey)
	setProperty(properties, prefix+".s3.session-token", firstNonEmpty(os.Getenv("ICEBERG_S3_SESSION_TOKEN"), os.Getenv("AWS_SESSION_TOKEN")))
	if pathStyle := firstNonEmpty(iceCfg.S3PathStyle, os.Getenv("ICEBERG_S3_PATH_STYLE"), os.Getenv("AWS_S3_PATH_STYLE")); pathStyle != "" {
		enabled, err := parseBoolEnvValue("iceberg s3_path_style", pathStyle)
		if err != nil {
			return nil, err
		}
		properties[prefix+".s3.path-style-access"] = strconv.FormatBool(enabled)
	}

	for key, value := range maintenanceCfg.SparkProperties {
		key = strings.TrimSpace(key)
		if key != "" {
			properties[key] = value
		}
	}
	return properties, nil
}

func setProperty(properties map[string]string, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		properties[key] = value
	}
}

func maintenanceAppName(jobID, jobName string) string {
	name := strings.TrimSpace(jobName)
	if name == "" {
		name = strings.TrimSpace(jobID)
	}
	if name == "" {
		name = "rivus"
	}
	return "rivus-iceberg-maintenance-" + name
}

func maintenanceCatalogName(iceCfg config.IcebergConfig) string {
	return firstNonEmpty(iceCfg.TableMaintenance.CatalogName, iceCfg.CatalogName, defaultSparkMaintenanceCatalog)
}

func normalizeMaintenanceOperation(operation string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(operation)), "-", "_")
}

func buildMaintenanceSQL(catalogName string, target config.IcebergTarget, operation string, options map[string]any) (string, error) {
	allowedOptions, ok := maintenanceOptionNames[operation]
	if !ok {
		return "", fmt.Errorf("unsupported maintenance operation %q", operation)
	}
	keys := make([]string, 0, len(options))
	normalizedOptions := make(map[string]any, len(options))
	for rawKey, value := range options {
		key := strings.ToLower(strings.TrimSpace(rawKey))
		if key == "table" {
			return "", fmt.Errorf("maintenance option table is managed by Rivus")
		}
		if _, allowed := allowedOptions[key]; !allowed {
			return "", fmt.Errorf("unsupported %s option %q", operation, rawKey)
		}
		if _, duplicate := normalizedOptions[key]; duplicate {
			return "", fmt.Errorf("duplicate %s option %q", operation, key)
		}
		normalizedOptions[key] = value
		keys = append(keys, key)
	}
	sort.Strings(keys)

	args := []string{"table => " + sparkSQLStringLiteral(tableKey(target.Namespace, target.Table))}
	for _, key := range keys {
		value := normalizedOptions[key]
		literal, err := maintenanceOptionLiteral(key, value)
		if err != nil {
			return "", fmt.Errorf("invalid %s option %s: %w", operation, key, err)
		}
		args = append(args, key+" => "+literal)
	}
	procedure := quoteSparkIdentifier(catalogName) + ".system." + quoteSparkIdentifier(operation)
	return "CALL " + procedure + "(" + strings.Join(args, ", ") + ")", nil
}

func maintenanceOptionLiteral(key string, value any) (string, error) {
	if key == "older_than" {
		timestamp, err := maintenanceTimestamp(value)
		if err != nil {
			return "", err
		}
		return "TIMESTAMP " + sparkSQLStringLiteral(timestamp.Format("2006-01-02 15:04:05.000000")), nil
	}
	return sparkSQLLiteral(value)
}

func sparkSQLLiteral(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return sparkSQLStringLiteral(typed), nil
	case bool:
		return strconv.FormatBool(typed), nil
	case json.Number:
		if _, err := strconv.ParseFloat(typed.String(), 64); err != nil {
			return "", fmt.Errorf("invalid number %q", typed.String())
		}
		return typed.String(), nil
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32), nil
	case int:
		return strconv.Itoa(typed), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case int32:
		return strconv.FormatInt(int64(typed), 10), nil
	case []any:
		items := make([]string, 0, len(typed))
		for _, item := range typed {
			literal, err := sparkSQLLiteral(item)
			if err != nil {
				return "", err
			}
			items = append(items, literal)
		}
		return "array(" + strings.Join(items, ", ") + ")", nil
	case []string:
		items := make([]string, 0, len(typed))
		for _, item := range typed {
			items = append(items, sparkSQLStringLiteral(item))
		}
		return "array(" + strings.Join(items, ", ") + ")", nil
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		items := make([]string, 0, len(keys)*2)
		for _, key := range keys {
			literal, err := sparkSQLLiteral(typed[key])
			if err != nil {
				return "", err
			}
			items = append(items, sparkSQLStringLiteral(key), literal)
		}
		return "map(" + strings.Join(items, ", ") + ")", nil
	case map[string]string:
		converted := make(map[string]any, len(typed))
		for key, item := range typed {
			converted[key] = item
		}
		return sparkSQLLiteral(converted)
	case nil:
		return "", fmt.Errorf("null is not supported")
	default:
		return "", fmt.Errorf("unsupported value type %T", value)
	}
}

func sparkSQLStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func quoteSparkIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func validateStreamingMaintenanceSafety(operation string, options map[string]any, now time.Time) error {
	if operation != "remove_orphan_files" {
		return nil
	}
	dryRunValue, _ := maintenanceOptionValue(options, "dry_run")
	if dryRun, ok := dryRunValue.(bool); ok && dryRun {
		return nil
	}
	olderThanValue, configured := maintenanceOptionValue(options, "older_than")
	if !configured {
		// Iceberg's Spark procedure defaults to three days, matching Rivus's
		// minimum safety window for an active writer.
		return nil
	}
	olderThan, err := maintenanceTimestamp(olderThanValue)
	if err != nil {
		return fmt.Errorf("invalid remove_orphan_files option older_than: %w", err)
	}
	if olderThan.After(now.Add(-defaultOrphanCleanupOlderThan)) {
		return fmt.Errorf("running iceberg jobs require remove_orphan_files older_than to be at least 72 hours old unless dry_run is true")
	}
	return nil
}

func maintenanceOptionValue(options map[string]any, wanted string) (any, bool) {
	for key, value := range options {
		if strings.EqualFold(strings.TrimSpace(key), wanted) {
			return value, true
		}
	}
	return nil, false
}

func maintenanceTimestamp(value any) (time.Time, error) {
	raw, ok := value.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("must be a timestamp string")
	}
	raw = strings.TrimSpace(raw)
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q; use RFC3339 or YYYY-MM-DD HH:MM:SS", raw)
}

func doSparkMaintenanceRequest(ctx context.Context, cfg config.IcebergTableMaintenanceConfig, method, path string, body any, response any) error {
	baseURL, err := sparkRESTBaseURL(cfg.SparkRESTURI)
	if err != nil {
		return err
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode Spark request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("create Spark request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	}
	if auth := strings.TrimSpace(cfg.RESTAuthHeader); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return &SparkMaintenanceError{Err: fmt.Errorf("Spark maintenance request failed: %w", err)}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSparkResponseBytes+1))
	if err != nil {
		return &SparkMaintenanceError{Err: fmt.Errorf("read Spark maintenance response: %w", err)}
	}
	if len(data) > maxSparkResponseBytes {
		return &SparkMaintenanceError{Err: fmt.Errorf("Spark maintenance response exceeds %d bytes", maxSparkResponseBytes)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(data))
		var sparkResponse sparkSubmissionResponse
		if json.Unmarshal(data, &sparkResponse) == nil && strings.TrimSpace(sparkResponse.Message) != "" {
			message = strings.TrimSpace(sparkResponse.Message)
		}
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		return &SparkMaintenanceError{Err: fmt.Errorf("Spark maintenance API returned %s: %s", resp.Status, message)}
	}
	if err := json.Unmarshal(data, response); err != nil {
		return &SparkMaintenanceError{Err: fmt.Errorf("decode Spark maintenance response: %w", err)}
	}
	return nil
}

func doRunnerMaintenanceRequest(ctx context.Context, cfg config.IcebergTableMaintenanceConfig, method, path string, body any, response any) error {
	baseURL, err := runnerBaseURL(maintenanceRunnerURI(cfg))
	if err != nil {
		return err
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode runner-app request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("create runner-app request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Runner-Token", maintenanceRunnerToken(cfg))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return &SparkMaintenanceError{Err: fmt.Errorf("runner-app maintenance request failed: %w", err)}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSparkResponseBytes+1))
	if err != nil {
		return &SparkMaintenanceError{Err: fmt.Errorf("read runner-app maintenance response: %w", err)}
	}
	if len(data) > maxSparkResponseBytes {
		return &SparkMaintenanceError{Err: fmt.Errorf("runner-app maintenance response exceeds %d bytes", maxSparkResponseBytes)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(data))
		var errorResponse struct {
			Detail any `json:"detail"`
		}
		if json.Unmarshal(data, &errorResponse) == nil && errorResponse.Detail != nil {
			message = fmt.Sprint(errorResponse.Detail)
		}
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		return &SparkMaintenanceError{Err: fmt.Errorf("runner-app maintenance API returned %s: %s", resp.Status, message)}
	}
	if err := json.Unmarshal(data, response); err != nil {
		return &SparkMaintenanceError{Err: fmt.Errorf("decode runner-app maintenance response: %w", err)}
	}
	return nil
}

func maintenanceUsesRunner(cfg config.IcebergTableMaintenanceConfig) bool {
	return maintenanceRunnerURI(cfg) != ""
}

func maintenanceRunnerURI(cfg config.IcebergTableMaintenanceConfig) string {
	if runnerURI := strings.TrimSpace(cfg.RunnerURI); runnerURI != "" {
		return runnerURI
	}
	// An explicit Spark endpoint is an intentional compatibility choice and
	// must not be shadowed by a process-wide runner URL.
	if strings.TrimSpace(cfg.SparkRESTURI) != "" {
		return ""
	}
	return firstNonEmpty(os.Getenv("RIVUS_RUNNER_URI"), os.Getenv("RUNNER_INTERNAL_BASE_URL"))
}

func maintenanceRunnerToken(cfg config.IcebergTableMaintenanceConfig) string {
	return firstNonEmpty(cfg.RunnerAPIToken, os.Getenv("RUNNER_API_TOKEN"))
}

func runnerBaseURL(raw string) (string, error) {
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", fmt.Errorf("invalid iceberg table_maintenance.runner_uri %q; expected an http(s) URL", raw)
	}
	return raw, nil
}

func sparkRESTBaseURL(raw string) (string, error) {
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", fmt.Errorf("invalid iceberg table_maintenance.spark_rest_uri %q; expected an http(s) URL", raw)
	}
	return raw, nil
}

func validateSparkSubmissionID(submissionID string) error {
	if !sparkSubmissionIDPattern.MatchString(strings.TrimSpace(submissionID)) {
		return fmt.Errorf("invalid Spark submission id %q", submissionID)
	}
	return nil
}
