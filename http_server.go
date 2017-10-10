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
Ceph status: {{.Status}}<br>
Used GiB: {{.UsedG}}<br>
FreeG: {{.FreeG}}<br>
DataG: {{.DataG}}<br>
ReadBPS: {{.ReadBPS}}<br>
WriteBPS: {{.WriteBPS}}<br>
ReadsPS: {{.ReadsPS}}<br>
WritesPS: {{.WritesPS}}<br>
OsdMapEpoch: {{.OsdMapEpoch}}<br>
</body></html>
`

type indexVars struct {
	Status string
	UsedG, FreeG, DataG, ReadBPS, WriteBPS, ReadsPS, WritesPS, OsdMapEpoch uint32
}

func (hcs *httpCephStatus) cephStatHandler(w http.ResponseWriter, r *http.Request) {
	indexParams := indexVars{}
	stat, err := hcs.cm.getStatus()
	if stat == nil {
		fmt.Fprintf(w, "<html><body>Error: failed to get ceph status: %v</body></html>", err)
		return
	}

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

	indexParams.UsedG = stat.UsedG
	indexParams.FreeG = stat.FreeG
	indexParams.DataG = stat.DataG
	indexParams.ReadBPS = stat.ReadBPS
	indexParams.WriteBPS = stat.WriteBPS
	indexParams.ReadsPS = stat.ReadsPS
	indexParams.WritesPS = stat.WritesPS
	indexParams.OsdMapEpoch = stat.OsdMapEpoch

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

func httpServer(addr string , cm *cephMonitor) {
	hcs := httpCephStatus{cm}
	http.HandleFunc("/", hcs.cephStatHandler) // set router
	err := http.ListenAndServe(addr, nil) // set listen port
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}