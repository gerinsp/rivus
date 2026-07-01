package connector

import (
	"context"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type JobContext struct {
	JobID      string
	JobName    string
	MetaKey    string
	Mode       config.JobMode
	StoredMode config.JobMode
	SinkType   string

	Retry     config.RetryPolicy
	MetaStore meta.OffsetStore

	Metadata       map[string]string
	ReportProgress ProgressReporter
}

type Source interface {
	Run(ctx context.Context, out chan<- model.Event) error
}

type Sink interface {
	Run(ctx context.Context, in <-chan model.Event) error
}

type ProgressInfo struct {
	Phase                   string
	Summary                 string
	Detail                  string
	CurrentTable            string
	CurrentTableIndex       int
	CompletedTables         int
	TotalTables             int
	CurrentTableRows        int64
	CDCStartFile            string
	CDCStartPos             uint32
	CDCCurrentFile          string
	CDCCurrentPos           uint32
	CheckpointPending       bool
	CheckpointReason        string
	CheckpointPosition      string
	CheckpointPendingTables string
}

type ProgressReporter func(ProgressInfo)

// Optional capabilities (engine akan cek via type assertion)

type TableRef struct {
	Schema string
	Table  string
}

type TableLister interface {
	Tables() []TableRef
}

type SchemaProvider interface {
	FetchSchema(ctx context.Context, schema, table string) (*model.TableSchema, error)
}

type TableRowCounter interface {
	CountRows(ctx context.Context, schema, table string) (int64, error)
}

type TableManager interface {
	EnsureTable(ctx context.Context, targetSchema, targetTable string, schema *model.TableSchema) error
}

type TargetResolver interface {
	ResolveTarget(srcSchema, srcTable string) (targetSchema, targetTable string)
}

type SchemaConsumer interface {
	RegisterSourceSchema(schema, table string, sourceSchema *model.TableSchema) error
}

type SnapshotTableSkipper interface {
	SkipSnapshotTables(tables []TableRef)
}

type SnapshotPrimaryKeySkipper interface {
	SkipSnapshotTableWithoutPrimaryKey(schema, table string, sourceSchema *model.TableSchema) bool
}

type TargetTableRowCounter interface {
	CountTargetRows(ctx context.Context, targetSchema, targetTable string) (int64, error)
}

type TargetTableResetter interface {
	ResetTargetTable(ctx context.Context, targetSchema, targetTable string) error
}
