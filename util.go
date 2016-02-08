package main

import "log"
import "os"

var logger = log.New(os.Stderr, "mysqlcsvdump:", log.LstdFlags)

var DEBUG bool
var QUIET bool

func debug(params ...interface{}) {
	if DEBUG {
		logger.Println(params...)
	}
}

func info(params ...interface{}) {
	if !QUIET {
		logger.Println(params...)
	}
}

func fatal(params ...interface{}) {
	logger.Fatalln(params...)
}

func handleError(err error) {
	if err != nil {
		fatal(err)
	}
}
