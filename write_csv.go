package main

import "fmt"
import "time"
import "database/sql"

type CsvWriter struct {
	*Config
}

func NewCsvWriter(config *Config) *CsvWriter {
	return &CsvWriter{config}
}

func (w *CsvWriter) WriteRow(row []interface{}) {
	for i, f := range row {
		if i != 0 {
			fmt.Print(w.FieldSep)
		}
		switch v := (f).(type) {
		case nil:
			fmt.Print(w.NullString)
		case bool:
			if v {
				fmt.Print("1")
			} else {
				fmt.Print("0")
			}
		case []byte:
			fmt.Print(string(v))
		case time.Time:
			if w.DateEpoch {
				fmt.Print(v.Unix())
			} else {
				fmt.Print(v.Format(time.RFC3339))
			}
		default:
			fmt.Print(v)
		}
	}
	fmt.Print(w.RowSep)
}

func (w *CsvWriter) WriteRows(rows *sql.Rows) {
	columnNames, err := rows.Columns()
	handleError(err)
	for i, c := range columnNames {
		if i != 0 {
			fmt.Print(w.FieldSep)
		}
		fmt.Print(c)
	}
	fmt.Print(w.RowSep)

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
		w.WriteRow(vals)
	}
}
