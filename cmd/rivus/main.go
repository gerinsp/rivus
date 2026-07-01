package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/api"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/connectors/doris"
	"github.com/gerinsp/rivus/pkg/connectors/iceberg"
	"github.com/gerinsp/rivus/pkg/connectors/mysql"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	uiDir := flag.String("ui-dir", "./ui", "UI directory")
	flag.Parse()

	logCloser, err := setupLogging()
	if err != nil {
		log.Fatalf("logging setup error: %v", err)
	}
	if logCloser != nil {
		defer logCloser.Close()
	}

	reg := connector.NewRegistry()
	mysql.Register(reg)
	doris.Register(reg)
	iceberg.Register(reg)

	jobManagerOpts := make([]core.JobManagerOption, 0, 2)
	if dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN")); dsn != "" {
		jobStore, err := meta.NewMySQLJobStore(dsn)
		if err != nil {
			log.Fatalf("job store error: %v", err)
		}
		jobManagerOpts = append(jobManagerOpts,
			core.WithJobStore(jobStore),
			core.WithDefaultMetaMySQLDSN(dsn),
		)
	}
	jobManager := core.NewJobManager(reg, jobManagerOpts...)
	restoreCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := jobManager.RestorePersistedJobs(restoreCtx); err != nil {
		log.Fatalf("restore persisted jobs failed: %v", err)
	}
	authConfig, err := api.LoadAuthConfigFromEnv()
	if err != nil {
		log.Fatalf("auth config error: %v", err)
	}

	apiServer := api.NewServer(jobManager, *uiDir, authConfig)
	mux := apiServer.Router()

	log.Printf("Starting rivus on %s ...", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func setupLogging() (io.Closer, error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := logConfigFromEnv()
	if !cfg.enabled {
		return nil, nil
	}

	w, err := newRotatingLogWriter(cfg)
	if err != nil {
		return nil, err
	}

	output := io.Writer(w)
	if cfg.stderrEnabled {
		output = io.MultiWriter(os.Stderr, w)
	}
	log.SetOutput(output)
	log.Printf("[logging] writing logs dir=%s prefix=%s retention_days=%d max_size_mb=%d max_total_size_mb=%d stderr=%t",
		cfg.dir, cfg.prefix, cfg.retentionDays, cfg.maxSizeMB, cfg.maxTotalSizeMB, cfg.stderrEnabled)
	return w, nil
}
