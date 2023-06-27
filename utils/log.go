package utils

import (
	"os"

	log "github.com/sirupsen/logrus"
)

func InitLog() {
	// TODO(Drogon): config logrus
	// init logrus
	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// log the debug severity or above.
	log.SetLevel(log.TraceLevel)

	// log filename linenumber func
	log.SetReportCaller(true)
}
