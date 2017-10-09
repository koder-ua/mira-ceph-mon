package main

import (
	"log"
	"net/http"
	"fmt"
	"html/template"
)

type httpCephStatus struct {
	cm * cephMonitor
}

const index = `<html><body>
Ceph status is {{.Status}}
</body></html>
`

type indexVars struct {
	Status string
}

func (hcs *httpCephStatus) cephStatHandler(w http.ResponseWriter, r *http.Request) {
	stat := hcs.cm.getStatus()
	if stat == nil {
		fmt.Fprintf(w, "<html><body>No data yet</body></html>")
	} else {
		indexParams := indexVars{}
		switch stat.Status {
		case HealthOk:
			indexParams.Status = "Ok"
		case HealthWarn:
			indexParams.Status = "Warning"
		case HealtheErr:
			indexParams.Status = "Error"
		default:
			panic("")
		}

		indexT, err := template.New("index").Parse(index)
		if err != nil {
			fmt.Fprintf(w, "<html><body>Error: failed to parse template %v</body></html>", err)
			return
		}

		err = indexT.Execute(w, indexParams)
		if err != nil {
			fmt.Fprintf(w, "<html><body>Error: failed to execute template %v</body></html>", err)
			return
		}
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