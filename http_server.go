package main

import (
	"log"
	"net/http"
	"fmt"
)

func cephStatHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "<html><body>Hi!</body></html>")
}

func httpServe(addr string , lm *latMonitor) {
	http.HandleFunc("/", cephStatHandler) // set router
	err := http.ListenAndServe(addr, nil) // set listen port
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}