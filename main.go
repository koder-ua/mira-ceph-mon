package main

import (
		"flag"
		"os/user"
		"io"
		"os"
		"log"
)

func main() {
	clusterName := flag.String("cluster", "ceph", "Set cluster name (ceph by default)")
	rpcAddr := flag.String("rpc", "", "Addr for grpc server")
	httpAddr := flag.String("http", "", "Addr for http server")
	osdID := flag.Int("osdid", -1, "OSD id")
	logFile := flag.String("logfile", "", "Store logs to file FILE")
	isSilent := flag.Bool("silent", false, "Don't log to stdout")
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
		log.SetOutput(logFD)
	} else {
		if logFD != nil {
			log.SetOutput(io.MultiWriter(os.Stdout, logFD))
		}
	}

	log.Printf("Starting ceph monitoring routines")
	lm := newLm()

	if *rpcAddr != "" {
		log.Printf("Starting rpc server at %s", *rpcAddr)
		rpcServer(*rpcAddr, lm)
	}

	if *httpAddr != "" {
		log.Printf("Starting http server at %s", *httpAddr)
		httpServe(*httpAddr, lm)
	}

	if *rpcAddr == "" {
		log.Print("Parsing historic ops for osd ", *osdID)
		lats, err := getLatList(*osdID, *clusterName, &map[string]bool{})
		if err != nil {
			log.Fatal("Failed to get lat list ", err)
		}
		log.Print(lats)
	} else {
		stat, err := getCephStatus(*clusterName)
		if err != nil {
			log.Fatal("Failed to get ceph status ", err)
		}
		log.Print(stat)
	}
}
