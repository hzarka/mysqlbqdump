package main

import "database/sql"
import "encoding/json"
import "time"
import "os"
import b64 "encoding/base64"
import "strings"

type JsonWriter struct {
	*Config
	enc *json.Encoder
	row map[string]interface{}
}

func NewJsonWriter(config *Config) *JsonWriter {
	return &JsonWriter{config, json.NewEncoder(os.Stdout), make(map[string]interface{})}
}

func (w *JsonWriter) WriteRow(columns []string, columnTypes []*sql.ColumnType, row []interface{}) {
	for i, c := range columns {
		switch v := (row[i]).(type) {
		case nil:
			w.row[c] = nil
		case bool:
			w.row[c] = v
		case []byte:
		    ctp := columnTypes[i]
		    if strings.Contains(strings.ToLower(ctp.DatabaseTypeName()), "binary") {
			    w.row[c] = b64.StdEncoding.EncodeToString(v)
		    } else {
			    w.row[c] = string(v)
		    }
		case time.Time:
			if w.DateEpoch {
				w.row[c] = v.Unix()
			} else {
				w.row[c] = v.Format(time.RFC3339)
			}
		default:
			w.row[c] = v
		}
	}
	w.enc.Encode(w.row)
}

func (w *JsonWriter) WriteRows(rows *sql.Rows) {
	columnNames, err := rows.Columns()
	handleError(err)

	columnTypes, err := rows.ColumnTypes()
	handleError(err)

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
		w.WriteRow(columnNames, columnTypes, vals)
	}
}
