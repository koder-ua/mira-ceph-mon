package main

import (
		//"flag"
		"os/user"
		"io"
		"os"
		"log"
		"gopkg.in/alecthomas/kingpin.v2"
)


var (
	app      = kingpin.New(os.Args[0], "Ceph historic ops latency & Co track daemon")
	rpcAddr  = app.Flag("rpc", "Addr for grpc server").PlaceHolder("IP:PORT").Short('r').String()
	httpAddr = app.Flag("http", "Addr for http server").PlaceHolder("IP:PORT").Short('h').String()
	logFile  = app.Flag("logfile", "Store logs to file FILE").Short('f').String()
	isSilent = app.Flag("silent", "Don't log to stdout").Short('q').Bool()
	logLevel = app.Flag("loglevel", "Log level (default = DEBUG)").Default("DEBUG").
		Enum("DEBUG", "INFO", "WARNING", "ERROR", "FATAL")
	ignoreNonRoot = app.Flag("ignorenonroot", "Don't fail if start not under root").Short('i').Bool()
)

func openLogFD(silent bool, fname string) (io.Writer, func()) {
	var fd io.Writer
	var logFD *os.File
	defer_close := func(){}

	if fname != "" {
		var err error
		logFD, err = os.OpenFile(fname, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Error opening file %s: %v", fname, err)
		}
		defer_close = func(){logFD.Close()}
	}

	if silent {
		if logFD == nil {
			var err error
			logFD, err = os.OpenFile("/dev/null", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644)
			if err != nil {
				log.Fatalf("Error opening file %s: %v", fname, err)
			}
			defer_close = func(){logFD.Close()}
		}
		fd = logFD
	} else {
		if logFD != nil {
			fd = io.MultiWriter(os.Stdout, logFD)
		}
	}
	return fd, defer_close
}

func main() {
	app.Version("0.0.1")
	app.Parse(os.Args[1:])

	if !*ignoreNonRoot {
		userInfo, err := user.Current()
		if err != nil {
			log.Fatalf("Failed to get current user: %v", err)
		}
		if userInfo.Username != "root" {
			log.Fatalf("Need to run as root")
		}
	}

	logFD, deferCall := openLogFD(*isSilent, *logFile)
	defer deferCall()
	setup_logging(*logLevel, logFD)

	clog.Printf("Starting ceph monitoring routines")

	if *rpcAddr == "" && *httpAddr == "" {
		clog.Panic("Either rpc or http addr need to be passed")
		panic("")
	}
	if *httpAddr != "" {
		clog.Info("Starting http server at ", *httpAddr)
	}

	if *httpAddr != "" {
		clog.Info("Starting rpc server at ", *rpcAddr)
	}

	cm := newCephMonitor()
	if *rpcAddr != "" && *httpAddr != "" {
		go httpServer(*httpAddr, cm)
		rpcServer(*rpcAddr, cm)
	} else if *rpcAddr != "" {
		rpcServer(*rpcAddr, cm)
	} else {
		httpServer(*httpAddr, cm)
	}
}
