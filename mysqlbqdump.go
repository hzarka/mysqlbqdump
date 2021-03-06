package main

import "gopkg.in/ini.v1"
import "log"
import "os"
import "fmt"
import "io/ioutil"
import "database/sql"
import flag "github.com/ogier/pflag"
import _ "github.com/go-sql-driver/mysql"

type Writer interface {
	WriteRows(*sql.Rows)
}

type Config struct {
	FieldSep           string
	RowSep             string
	NullString         string
	OutputDir          string
	OutputFileRowLimit int
	DateEpoch          bool
}

func getDSN(filename string, section string, database string) string {
	debug("importing", filename, section)
	cfg, err := ini.Load(filename)
	if err != nil {
		log.Fatalln(err)
	}
	sec := cfg.Section(section)
	host := sec.Key("host").MustString("127.0.0.1")
	port := sec.Key("port").MustString("3306")
	user := sec.Key("user").String()
	password := sec.Key("password").String()
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, database)
}

func main() {
	var config Config
	flag.BoolVar(&DEBUG, "debug", false, "enable debug logging")
	flag.BoolVar(&QUIET, "quiet", false, "disable output")
	flag.StringVar(&config.FieldSep, "csv-fields-terminated-by", "\t", "field separator")
	flag.StringVar(&config.RowSep, "csv-records-terminated-by", "\n", "row separator")
	flag.StringVar(&config.NullString, "csv-null-string", "\\N", "output string for NULL values")
	flag.StringVar(&config.OutputDir, "output-dir", "\\N", "output directory")
	flag.IntVar(&config.OutputFileRowLimit, "output-file-row-limit", 0, "output file row limit")
	flag.BoolVar(&config.DateEpoch, "epoch", true, "output datetime as epoch instead of RFC3339")
	defaults_file := flag.String("defaults-file", "my.cnf", "defaults file")
	defaults_group_suffix := flag.String("defaults-group-suffix", "", "defaults group suffix")
	format := flag.String("format", "json", "output format 'json' or 'csv' or 'avro' (experimental)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: mysqlbqdump [options] database table > output.json\n\n")
		fmt.Fprintf(os.Stderr, "Reads connection info from ./my.cnf. Use '-' for table to send query in stdin\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
		os.Exit(1)
	}

	if config.OutputFileRowLimit > 0 {
		if config.OutputDir == "\\N" {
			log.Fatalln("cant specify output-file-row-limit without output-dir")
		}
		if *format != "avro" {
			log.Fatalln("only avro supported for multi-file export")
		}
		err := os.MkdirAll(config.OutputDir, os.ModePerm)
		handleError(err)
	}

	dsn := getDSN(*defaults_file, "client"+*defaults_group_suffix, args[0])
	rows := getRows(dsn, args[1])
	if *format == "json" {
		NewJsonWriter(&config).WriteRows(rows)
	} else if *format == "avro" {
		NewAvroWriter(&config).WriteRows(rows)
	} else {
		NewCsvWriter(&config).WriteRows(rows)
	}
}

func getRows(dsn string, table string) *sql.Rows {
	db, err := sql.Open("mysql", dsn)
	handleError(err)
	defer db.Close()
	query := fmt.Sprintf("SELECT * FROM `%s`", table)
	if table == "-" {
		bytes, err := ioutil.ReadAll(os.Stdin)
		handleError(err)
		query = string(bytes)
	}
	stmt, err := db.Prepare(query)
	handleError(err)
	defer stmt.Close()
	rows, err := stmt.Query()
	handleError(err)
	return rows
}
