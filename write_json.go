package main

import "database/sql"
import "encoding/json"
import "time"
import "os"

type JsonWriter struct {
	*Config
	enc *json.Encoder
	row map[string]interface{}
}

func NewJsonWriter(config *Config) *JsonWriter {
	return &JsonWriter{config, json.NewEncoder(os.Stdout), make(map[string]interface{})}
}

func (w *JsonWriter) WriteRow(columns []string, row []interface{}) {
	for i, c := range columns {
		switch v := (row[i]).(type) {
		case nil:
			w.row[c] = nil
		case bool:
			w.row[c] = v
		case []byte:
			w.row[c] = string(v)
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
		w.WriteRow(columnNames, vals)
	}
}
