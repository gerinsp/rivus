package model

import "time"

type EventType string

const (
	EventTypeInsert        EventType = "INSERT"
	EventTypeUpdate        EventType = "UPDATE"
	EventTypeDelete        EventType = "DELETE"
	EventTypeDDL           EventType = "DDL"
	EventTypeCheckpoint    EventType = "CHECKPOINT"
	EventTypeSnapshotBatch EventType = "SNAPSHOT_BATCH"
)

type EventOrigin string

const (
	EventOriginSnapshot EventOrigin = "SNAPSHOT"
	EventOriginCDC      EventOrigin = "CDC"
)

type SourceOffset struct {
	BinlogFile string `json:"binlog_file,omitempty"`
	BinlogPos  uint32 `json:"binlog_pos,omitempty"`
}

func (o *SourceOffset) Valid() bool {
	return o != nil && o.BinlogFile != "" && o.BinlogPos > 0
}

type Event struct {
	Type         EventType                `json:"type"`
	TraceID      string                   `json:"trace_id,omitempty"`
	Schema       string                   `json:"schema,omitempty"`
	Table        string                   `json:"table,omitempty"`
	Data         map[string]interface{}   `json:"data,omitempty"`
	Rows         []map[string]interface{} `json:"rows,omitempty"`
	OldData      map[string]interface{}   `json:"old_data,omitempty"`
	DDL          string                   `json:"ddl,omitempty"`
	Timestamp    time.Time                `json:"timestamp"`
	Origin       EventOrigin              `json:"origin,omitempty"`
	SourceOffset *SourceOffset            `json:"source_offset,omitempty"`
	SourceSchema *TableSchema             `json:"source_schema,omitempty"`
	// SnapshotStartOffset is the row offset before this snapshot batch.
	SnapshotStartOffset int64      `json:"snapshot_start_offset,omitempty"`
	Ack                 chan error `json:"-"`
}

type TableColumn struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`             // ex: varchar, text, decimal
	ColumnType string `json:"column_type,omitempty"` // ex: varchar(255), decimal(10,2)

	CharMaxLen *int64 `json:"char_max_len,omitempty"` // for char/varchar/text
	NumPrec    *int64 `json:"num_prec,omitempty"`     // for decimal, numeric
	NumScale   *int64 `json:"num_scale,omitempty"`

	IsNullable bool `json:"is_nullable"`
	IsPK       bool `json:"is_pk"`
}

type TableSchema struct {
	SchemaName string        `json:"schema_name"`
	TableName  string        `json:"table_name"`
	Columns    []TableColumn `json:"columns"`
}
