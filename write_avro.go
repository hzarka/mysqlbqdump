package main

import "database/sql"
import "os"
import goavro "gopkg.in/linkedin/goavro.v2"
import "strings"
import "fmt"
import "time"
import "math/big"

type AvroWriter struct {
	*Config
	enc *goavro.OCFWriter
	row map[string]interface{}
}

func NewAvroWriter(config *Config) *AvroWriter {
	return &AvroWriter{config, nil, make(map[string]interface{})}
}

func (w *AvroWriter) WriteRow(columns []string, converters []convertfn, row []interface{}) {
	for i, fn := range converters {
		w.row[columns[i]] = fn(row[i])
	}
	err := w.enc.Append([]interface{}{w.row})
	handleError(err)
}

func (w *AvroWriter) WriteRows(rows *sql.Rows) {
	columnNames, err := rows.Columns()
	handleError(err)

	columnTypes, err := rows.ColumnTypes()
	handleError(err)

	fns, writer := createWriter(columnTypes)
	w.enc = writer

	vals := make([]interface{}, len(columnNames))
	scanArgs := make([]interface{}, len(columnNames))
	for i := 0; i < len(columnNames); i++ {
		scanArgs[i] = &vals[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			fatal(err)
		}
		w.WriteRow(columnNames, fns, vals)
	}
}

func createWriter(columnTypes []*sql.ColumnType) ([]convertfn, *goavro.OCFWriter) {
	fns, schema := getAvroSchema(columnTypes)
	config := goavro.OCFConfig{os.Stdout, nil, schema, "snappy", nil}
	ret, err := goavro.NewOCFWriter(config)
	handleError(err)

	return fns, ret
}

func getAvroSchema(columnTypes []*sql.ColumnType) ([]convertfn, string) {
	fns := make([]convertfn, len(columnTypes))

	var sb strings.Builder
	sb.WriteString(` { "namespace": "bqdump", "type": "record", "name": "tbl","fields": [ `)
	for i, ctype := range columnTypes {
		if i > 0 {
			sb.WriteString(",")
		}
		fn, s := getAvroTypeFromMysqlType(ctype)
		fns[i] = fn
		sb.WriteString(fmt.Sprintf(s, ctype.Name()))
	}
	sb.WriteString(` ] } `)
	return fns, sb.String()
}

// Due to bug (in goavro library? or library misuse here?):
// - with multiple decimal specifications, only the first spec seems to be used for big.Rat -> bytes encoding
// - but we write the avro schema correctly for each field
// - so readers interpret the byte values with incorrect scale/precision
// - for now, we'll just reuse the first decimal type for all decimal types in the schema
//
// This could lead to loss of precision, but better than having wrong values for now
var FIRST_DECIMAL_TYPE string = "";

func getAvroTypeFromMysqlType(ctype *sql.ColumnType) (convertfn, string) {
	dbt := strings.ToLower(ctype.DatabaseTypeName())
	if dbt == "date" {
		return typeFns["date"], typeJsons["date"]
	}
	if dbt == "datetime" {
		return typeFns["timestamp"], typeJsons["timestamp"]
	}
	if dbt == "timestamp" {
		return typeFns["timestamp"], typeJsons["timestamp"]
	}
	if dbt == "decimal" {
		precision, scale, _ := ctype.DecimalSize()
		if FIRST_DECIMAL_TYPE == "" {
		    FIRST_DECIMAL_TYPE = fmt.Sprintf(typeJsons["decimal"], precision, scale)
		}
		return typeFns["decimal"], FIRST_DECIMAL_TYPE
	}
	if dbt == "double" || dbt == "float" {
		return typeFns["double"], typeJsons["double"]
	}
	if strings.HasSuffix(dbt, "int") {
		return typeFns["long"], typeJsons["long"]
	}
	if strings.Contains(dbt, "binary") || strings.Contains(dbt, "blob") {
		return typeFns["bytes"], typeJsons["bytes"]
	}
	if strings.Contains(dbt, "text") || strings.Contains(dbt, "char") || dbt == "json" || dbt == "enum" {
		return typeFns["string"], typeJsons["string"]
	}
	fatal("unknown type %s", ctype)
	return nil, ""
}

type convertfn func(interface{}) interface{}

func convert_default(v interface{}) interface{} {
	return nil
}

func convert_long(v interface{}) interface{} {
	switch v := v.(type) {
	case nil:
		return nil
	default:
		return goavro.Union("long", v)
	}
	return nil
}

func convert_double(v interface{}) interface{} {
	switch v := v.(type) {
	case nil:
		return nil
	default:
		return goavro.Union("double", v)
	}
	return nil
}

func convert_decimal(v interface{}) interface{} {
	switch v := v.(type) {
	case []byte:
		r := new(big.Rat)
		r.SetString(string(v))
		return goavro.Union("bytes.decimal", r)
	case nil:
		return nil
	default:
		fatal("bad type for decimal", v)
	}
	return nil
}

func convert_string(v interface{}) interface{} {
	switch v := v.(type) {
	case []byte:
		return goavro.Union("string", v)
	case nil:
		return nil
	default:
		fatal("bad type for string", v)
	}
	return nil
}

func convert_bytes(v interface{}) interface{} {
	switch v := v.(type) {
	case []byte:
		return goavro.Union("bytes", v)
	case nil:
		return nil
	default:
		fatal("bad type for bytes", v)
	}
	return nil
}

func convert_date(v interface{}) interface{} {
	switch v := v.(type) {
	case time.Time:
		return goavro.Union("int.date", v)
	case nil:
		return nil
	default:
		fatal("bad type for date", v)
	}
	return nil
}

func convert_timestamp(v interface{}) interface{} {
	switch v := v.(type) {
	case time.Time:
		return goavro.Union("long.timestamp-millis", v)
	case nil:
		return nil
	default:
		fatal("bad type for timestamp", v)
	}
	return nil
}

var typeJsons = map[string]string{
	"date":      ` {"name": "%s",  "type": ["null", {"type": "int", "logicalType": "date"}]} `,
	"timestamp": ` {"name": "%s",  "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]} `,
	"decimal":   ` {"name": "%%s",  "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": %d, "scale": %d}]} `,
	"double":    ` {"name": "%s",  "type": ["double", "null"]} `,
	"long":      ` {"name": "%s",  "type": ["long", "null"]} `,
	"bytes":     ` {"name": "%s",  "type": ["bytes", "null"]} `,
	"string":    ` {"name": "%s",  "type": ["string", "null"]} `,
}

var typeFns = map[string]convertfn{
	"date":      convert_date,
	"timestamp": convert_timestamp,
	"decimal":   convert_decimal,
	"double":    convert_double,
	"long":      convert_long,
	"bytes":     convert_bytes,
	"string":    convert_string,
}
