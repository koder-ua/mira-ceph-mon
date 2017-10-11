package main

import (
	"github.com/sirupsen/logrus"
	"io"
)


var clog = logrus.New()

func setup_logging(level string, output io.Writer) {
	clog.Formatter = new(logrus.TextFormatter)
	switch level {
	case "DEBUG":
		clog.Level = logrus.DebugLevel
	case "INFO":
		clog.Level = logrus.InfoLevel
	case "WARNING":
		clog.Level = logrus.WarnLevel
	case "ERROR":
		clog.Level = logrus.ErrorLevel
	case "PANIC":
		clog.Level = logrus.PanicLevel
	}
	clog.Out = output
}

