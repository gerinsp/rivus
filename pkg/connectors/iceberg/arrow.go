package iceberg

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/model"
)

type pendingRow struct {
	key   map[string]interface{}
	row   map[string]interface{}
	event model.Event
	pos   int
}

type pendingDelete struct {
	key           map[string]interface{}
	row           map[string]interface{}
	partitionRows []map[string]interface{}
	pos           int
}

func reduceEvents(events []model.Event, pkCols []string) ([]map[string]interface{}, []map[string]interface{}, error) {
	rows, deleteRows, err := reduceEventsDetailed(events, pkCols)
	if err != nil {
		return nil, nil, err
	}

	outRows := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		outRows = append(outRows, row.row)
	}

	outDeletes := make([]map[string]interface{}, 0, len(deleteRows))
	for _, row := range deleteRows {
		outDeletes = append(outDeletes, row.key)
	}

	return outRows, outDeletes, nil
}

func reduceEventsDetailed(events []model.Event, pkCols []string) ([]pendingRow, []pendingDelete, error) {
	rowsByKey := make(map[string]pendingRow)
	deletesByKey := make(map[string]pendingDelete)

	for idx, ev := range events {
		switch ev.Type {
		case model.EventTypeInsert:
			key, hash, err := keyFromRow(ev.Data, pkCols)
			if err != nil {
				return nil, nil, err
			}
			addPendingDelete(deletesByKey, hash, pendingDelete{key: key, row: cloneMap(ev.Data), pos: idx})
			rowsByKey[hash] = pendingRow{key: key, row: cloneMap(ev.Data), event: ev, pos: idx}
		case model.EventTypeDelete:
			key, hash, err := keyFromRow(ev.Data, pkCols)
			if err != nil {
				return nil, nil, err
			}
			delete(rowsByKey, hash)
			addPendingDelete(deletesByKey, hash, pendingDelete{key: key, row: cloneMap(ev.Data), pos: idx})
		case model.EventTypeUpdate:
			oldKeyData := ev.OldData
			if oldKeyData == nil {
				oldKeyData = ev.Data
			}
			oldKey, oldHash, err := keyFromRow(oldKeyData, pkCols)
			if err != nil {
				return nil, nil, err
			}
			newKey, newHash, err := keyFromRow(ev.Data, pkCols)
			if err != nil {
				return nil, nil, err
			}

			delete(rowsByKey, oldHash)
			addPendingDelete(deletesByKey, oldHash, pendingDelete{key: oldKey, row: cloneMap(oldKeyData), pos: idx})
			rowsByKey[newHash] = pendingRow{key: newKey, row: cloneMap(ev.Data), event: ev, pos: idx}
		}
	}

	rows := make([]pendingRow, 0, len(rowsByKey))
	for _, row := range rowsByKey {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].pos < rows[j].pos })

	deleteRows := make([]pendingDelete, 0, len(deletesByKey))
	for _, row := range deletesByKey {
		deleteRows = append(deleteRows, row)
	}
	sort.Slice(deleteRows, func(i, j int) bool { return deleteRows[i].pos < deleteRows[j].pos })

	return rows, deleteRows, nil
}

func addPendingDelete(deletesByKey map[string]pendingDelete, hash string, del pendingDelete) {
	del.partitionRows = append([]map[string]interface{}{}, cloneMap(del.row))
	if existing, ok := deletesByKey[hash]; ok {
		existing.partitionRows = append(existing.partitionRows, del.partitionRows...)
		existing.row = del.row
		existing.pos = del.pos
		deletesByKey[hash] = existing
		return
	}
	deletesByKey[hash] = del
}

func buildKeyFilter(schema *iceberglib.Schema, keys []map[string]interface{}) (iceberglib.BooleanExpression, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	var filter iceberglib.BooleanExpression
	for _, key := range keys {
		var rowFilter iceberglib.BooleanExpression
		for col, value := range key {
			field, ok := schema.FindFieldByNameCaseInsensitive(col)
			if !ok {
				return nil, fmt.Errorf("field %s not found in iceberg schema", col)
			}
			lit, err := literalForValue(field.Type, value)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", col, err)
			}
			pred := iceberglib.LiteralPredicate(iceberglib.OpEQ, iceberglib.Reference(field.Name), lit)
			if rowFilter == nil {
				rowFilter = pred
			} else {
				rowFilter = iceberglib.NewAnd(rowFilter, pred)
			}
		}

		if filter == nil {
			filter = rowFilter
		} else {
			filter = iceberglib.NewOr(filter, rowFilter)
		}
	}

	return filter, nil
}

func buildRecordReader(schema *iceberglib.Schema, rows []map[string]interface{}) (array.RecordReader, func(), error) {
	arrSchema, err := icetable.SchemaToArrowSchema(schema, nil, false, false)
	if err != nil {
		return nil, nil, err
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrSchema)
	release := func() { bldr.Release() }

	for _, row := range rows {
		for idx, field := range arrSchema.Fields() {
			value, _ := lookupColumnValue(row, field.Name)
			if err := appendBuilderValue(bldr.Field(idx), field.Type, value); err != nil {
				release()
				return nil, nil, fmt.Errorf("column %s: %w", field.Name, err)
			}
		}
	}

	batch := bldr.NewRecordBatch()
	reader, err := array.NewRecordReader(arrSchema, []arrow.RecordBatch{batch})
	if err != nil {
		batch.Release()
		release()
		return nil, nil, err
	}

	return reader, func() {
		reader.Release()
		batch.Release()
		release()
	}, nil
}

func appendBuilderValue(builder array.Builder, dataType arrow.DataType, value interface{}) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch typ := dataType.(type) {
	case *arrow.BooleanType:
		v, err := toBool(value)
		if err != nil {
			return err
		}
		builder.(*array.BooleanBuilder).Append(v)
		return nil
	case *arrow.Int32Type:
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		builder.(*array.Int32Builder).Append(int32(v))
		return nil
	case *arrow.Int64Type:
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		builder.(*array.Int64Builder).Append(v)
		return nil
	case *arrow.Float32Type:
		v, err := toFloat64(value)
		if err != nil {
			return err
		}
		builder.(*array.Float32Builder).Append(float32(v))
		return nil
	case *arrow.Float64Type:
		v, err := toFloat64(value)
		if err != nil {
			return err
		}
		builder.(*array.Float64Builder).Append(v)
		return nil
	case *arrow.StringType:
		builder.(*array.StringBuilder).Append(toString(value))
		return nil
	case *arrow.BinaryType:
		builder.(*array.BinaryBuilder).Append(toBytes(value))
		return nil
	case *arrow.TimestampType:
		if isMySQLZeroDateValue(value) {
			builder.AppendNull()
			return nil
		}
		tm, err := toTime(value)
		if err != nil {
			return err
		}
		ts, err := arrow.TimestampFromTime(tm.UTC(), typ.Unit)
		if err != nil {
			return err
		}
		builder.(*array.TimestampBuilder).Append(ts)
		return nil
	case *arrow.Date32Type:
		if isMySQLZeroDateValue(value) {
			builder.AppendNull()
			return nil
		}
		tm, err := toTime(value)
		if err != nil {
			return err
		}
		builder.(*array.Date32Builder).Append(arrow.Date32FromTime(tm.UTC()))
		return nil
	case *arrow.Decimal128Type:
		dec, err := decimal128.FromString(toString(value), typ.Precision, typ.Scale)
		if err != nil {
			return err
		}
		builder.(*array.Decimal128Builder).Append(dec)
		return nil
	default:
		return fmt.Errorf("unsupported arrow type %T", dataType)
	}
}

func literalForValue(typ iceberglib.Type, value interface{}) (iceberglib.Literal, error) {
	if _, ok := typ.(iceberglib.DateType); ok {
		return literalForDateValue(value, typ)
	}
	if _, ok := typ.(iceberglib.TimestampType); ok {
		return literalForTimestampValue(value, typ)
	}
	if _, ok := typ.(iceberglib.TimestampTzType); ok {
		return literalForTimestampValue(value, typ)
	}

	switch v := value.(type) {
	case nil:
		return nil, fmt.Errorf("nil literal is not allowed for key filters")
	case bool:
		return iceberglib.NewLiteral(v).To(typ)
	case int:
		return iceberglib.NewLiteral(int64(v)).To(typ)
	case int8:
		return iceberglib.NewLiteral(int64(v)).To(typ)
	case int16:
		return iceberglib.NewLiteral(int64(v)).To(typ)
	case int32:
		return iceberglib.NewLiteral(v).To(typ)
	case int64:
		return iceberglib.NewLiteral(v).To(typ)
	case uint:
		return iceberglib.NewLiteral(int64(v)).To(typ)
	case uint8:
		return iceberglib.NewLiteral(int64(v)).To(typ)
	case uint16:
		return iceberglib.NewLiteral(int64(v)).To(typ)
	case uint32:
		return iceberglib.NewLiteral(int64(v)).To(typ)
	case uint64:
		return iceberglib.NewLiteral(int64(v)).To(typ)
	case float32:
		return iceberglib.NewLiteral(v).To(typ)
	case float64:
		return iceberglib.NewLiteral(v).To(typ)
	case string:
		return iceberglib.StringLiteral(v).To(typ)
	case []byte:
		if _, ok := typ.(iceberglib.BinaryType); ok {
			return iceberglib.BinaryLiteral(v).To(typ)
		}
		return iceberglib.StringLiteral(string(v)).To(typ)
	case json.Number:
		return iceberglib.StringLiteral(v.String()).To(typ)
	case time.Time:
		return iceberglib.StringLiteral(v.UTC().Format(time.RFC3339Nano)).To(typ)
	default:
		return iceberglib.StringLiteral(fmt.Sprint(v)).To(typ)
	}
}

func literalForTimestampValue(value interface{}, typ iceberglib.Type) (iceberglib.Literal, error) {
	if value == nil {
		return nil, fmt.Errorf("nil literal is not allowed for key filters")
	}

	tm, err := toTime(value)
	if err != nil {
		return nil, err
	}
	switch typ.(type) {
	case iceberglib.TimestampType:
		return iceberglib.StringLiteral(tm.UTC().Format("2006-01-02T15:04:05")).To(typ)
	case iceberglib.TimestampTzType:
		return iceberglib.StringLiteral(tm.UTC().Format(time.RFC3339)).To(typ)
	default:
		return iceberglib.StringLiteral(tm.UTC().Format(time.RFC3339Nano)).To(typ)
	}
}

func literalForDateValue(value interface{}, typ iceberglib.Type) (iceberglib.Literal, error) {
	if value == nil {
		return nil, fmt.Errorf("nil literal is not allowed for key filters")
	}

	tm, err := toTime(value)
	if err != nil {
		return nil, err
	}
	return iceberglib.StringLiteral(tm.UTC().Format("2006-01-02")).To(typ)
}

func keyFromRow(data map[string]interface{}, pkCols []string) (map[string]interface{}, string, error) {
	key := make(map[string]interface{}, len(pkCols))
	parts := make([]string, 0, len(pkCols))
	for _, col := range pkCols {
		value, ok := lookupColumnValue(data, col)
		if !ok {
			return nil, "", fmt.Errorf("missing primary key value for %s", col)
		}
		key[col] = value
		parts = append(parts, fmt.Sprintf("%s=%v", strings.ToLower(col), value))
	}
	return key, strings.Join(parts, "|"), nil
}

func cloneMap(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func lookupColumnValue(data map[string]interface{}, col string) (interface{}, bool) {
	if data == nil {
		return nil, false
	}
	if value, ok := data[col]; ok {
		return value, true
	}
	for key, value := range data {
		if strings.EqualFold(strings.TrimSpace(key), strings.TrimSpace(col)) {
			return value, true
		}
	}
	return nil, false
}

func toString(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format("2006-01-02 15:04:05.999999")
	case json.Number:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

func toBytes(value interface{}) []byte {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return append([]byte(nil), v...)
	case string:
		return []byte(v)
	default:
		return []byte(fmt.Sprint(v))
	}
}

func toBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		return fmt.Sprint(v) != "0", nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(v) != "0", nil
	case string:
		return strconv.ParseBool(strings.TrimSpace(v))
	case []byte:
		return strconv.ParseBool(strings.TrimSpace(string(v)))
	case json.Number:
		return v.String() != "0", nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

func toInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	case []byte:
		return strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
	case json.Number:
		return v.Int64()
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

func toFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(v), 64)
	case []byte:
		return strconv.ParseFloat(strings.TrimSpace(string(v)), 64)
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func toTime(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		return parseTimeString(v)
	case []byte:
		return parseTimeString(string(v))
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time", value)
	}
}

func isMySQLZeroDateValue(value interface{}) bool {
	switch v := value.(type) {
	case string:
		return isMySQLZeroDateString(v)
	case []byte:
		return isMySQLZeroDateString(string(v))
	default:
		return false
	}
}

func isMySQLZeroDateString(raw string) bool {
	raw = strings.TrimSpace(raw)
	return raw == "0000-00-00" ||
		strings.HasPrefix(raw, "0000-00-00 ") ||
		strings.HasPrefix(raw, "0000-00-00T")
}

func parseTimeString(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time value %q", raw)
}
