package main

import "database/sql"
import "os"
import goavro "gopkg.in/linkedin/goavro.v2"
import "strings"
import "fmt"
import "time"

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

	//info("append", w.row)
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
        precision, _, _ := ctype.DecimalSize()
	    return typeFns["decimal"], fmt.Sprintf(typeJsons["decimal"], precision)
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
    return goavro.Union("long", v)
}

func convert_double(v interface{}) interface{} {
    return goavro.Union("double", v)
}

func convert_decimal(v interface{}) interface{} {
    return goavro.Union("bytes", v)
}

func convert_string(v interface{}) interface{} {
    switch v := v.(type) {
	case []byte:
        return goavro.Union("string", v)
    default:
        fatal("bad type", v)
    }
    return nil
}

func convert_bytes(v interface{}) interface{} {
    switch v := v.(type) {
	case []byte:
        return goavro.Union("bytes", v)
    default:
        fatal("bad type", v)
    }
    return nil
}

func convert_date(v interface{}) interface{} {
    switch v := v.(type) {
	case time.Time:
	    days := int64(v.Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)) / time.Hour) / 24
        return goavro.Union("int", days)
    default:
        fatal("bad type", v)
    }
    return nil
}

func convert_timestamp(v interface{}) interface{} {
    switch v := v.(type) {
	case time.Time:
        return goavro.Union("long", v.UnixNano() / int64(time.Millisecond))
    default:
        fatal("bad type", v)
    }
    return nil
}

var typeJsons = map[string]string{
	"date": ` {"name": "%s",  "type": ["int", "null"], "logicalType": "date"} `,
	"timestamp": ` {"name": "%s",  "type": ["long", "null"], "logicalType": "timestamp-millis"} `,
	"decimal": ` {"name": "%%s",  "type": ["bytes", "null"],  "logicalType": "decimal", "scale": 0, "precision": %d } `,
	"double": ` {"name": "%s",  "type": ["double", "null"]} `,
	"long": ` {"name": "%s",  "type": ["long", "null"]} `,
	"bytes": ` {"name": "%s",  "type": ["bytes", "null"]} `,
	"string": ` {"name": "%s",  "type": ["string", "null"]} `,
}

var typeFns = map[string]convertfn{
	"date": convert_date,
	"timestamp": convert_timestamp,
	"decimal": convert_decimal,
	"double": convert_double,
	"long": convert_long,
	"bytes": convert_bytes,
	"string": convert_string,
}


