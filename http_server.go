package main

import (
	"log"
	"net/http"
	"fmt"
)

type httpCephStatus struct {
	cm * cephMonitor
}

func (hcs *httpCephStatus) cephStatHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	stat := hcs.cm.getStatus()
	if stat == nil {
		fmt.Fprintf(w, "<html><body>Ceph status is unknown yet</body></html>")
	} else {
		fmt.Fprintf(w, "<html><body>Ceph status is: %d </body></html>", stat.Status)
	}
}

func httpServer(addr string , cm *cephMonitor) {
	hcs := httpCephStatus{cm}
	http.HandleFunc("/", hcs.cephStatHandler) // set router
	err := http.ListenAndServe(addr, nil) // set listen port
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}