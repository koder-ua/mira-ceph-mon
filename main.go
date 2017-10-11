package main

import (
		"flag"
		"os/user"
		"io"
		"os"
		"log"
)

func main() {
	rpcAddr := flag.String("rpc", "", "Addr for grpc server")
	httpAddr := flag.String("http", "", "Addr for http server")
	logFile := flag.String("logfile", "", "Store logs to file FILE")
	isSilent := flag.Bool("silent", false, "Don't log to stdout")
	logLevel := flag.String("loglevel", "DEBUG", "Log level (default = DEBUG)")
	ignoreNonRoot := flag.Bool("ignorenonroot", false, "Don't fail if start not under root")
	flag.Parse()

	if !*ignoreNonRoot {
		userInfo, err := user.Current()
		if err != nil {
			log.Fatalf("Failed to get current user: %v", err)
		}
		if userInfo.Username != "root" {
			log.Fatalf("Need to run as root")
		}
	}

	var logFD *os.File

	if *logFile != "" {
		var err error
		logFD, err = os.OpenFile(*logFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Error opening file %s: %v", *logFile, err)
		}
		defer logFD.Close()
	}

	if *isSilent {
		if logFD == nil {
			var err error
			logFD, err = os.OpenFile("/dev/null", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644)
			if err != nil {
				log.Fatalf("Error opening file %s: %v", *logFile, err)
			}
			defer logFD.Close()
		}
		//log.SetOutput(logFD)
		setup_logging(*logLevel, logFD)
	} else {
		if logFD != nil {
			//log.SetOutput(io.MultiWriter(os.Stdout, logFD))
			setup_logging(*logLevel, io.MultiWriter(os.Stdout, logFD))
		}
	}

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
