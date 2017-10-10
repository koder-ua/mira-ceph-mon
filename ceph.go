package main

import ("math"
		"log"
		"github.com/golang/protobuf/proto"
		"os/exec"
		"fmt"
		"strconv"
		"strings"
		"time"
		"sync"
)

// Ceph health constants
const (
	// make lint happy
	HealthOk   = iota
	// make lint happy
	HealthWarn = iota
	// make lint happy
	HealtheErr = iota
)


type cephLats struct {
	rlats, wlats []uint32
}

type cephLatsHisto struct {
	rlats, wlats []uint32
}

type monitoringConfig struct {
	running bool
	cluster string
	osdIDS []int
	timeout int
	min, max, bins uint32
	done chan struct{}
}

type cephMonitor struct {
	cluster string
	osdIDs []int
	latsHistoChan chan *cephLatsHisto
	latsHistoChanNoClear chan *cephLatsHisto
	cephStatChan chan *CephStatus
	monitoringConfigChan chan *monitoringConfig
	quit chan struct{}
	wg sync.WaitGroup
	timeout int
}


func newCephMonitor() *cephMonitor {
	cm := &cephMonitor{
		latsHistoChan: make(chan *cephLatsHisto),
		latsHistoChanNoClear: make(chan *cephLatsHisto),
		cephStatChan: make(chan *CephStatus),
		monitoringConfigChan: make(chan *monitoringConfig),
		quit: make(chan struct{}),
		osdIDs: make([]int, 0),
	}
	go cm.configFiber()
	return cm
}


func (cm *cephMonitor) configFiber() {
	log.Printf("Ceph config fiber started")
	running := false

	// start with stubs
	latsHistoChan := cm.latsHistoChan
	latsHistoChanNoClear := cm.latsHistoChanNoClear
	cephStatChan := cm.cephStatChan

	for {
		select {
		case cfg := <-cm.monitoringConfigChan:
			if cfg == nil {
				log.Printf("Ceph config fiber exits")
				return
			}

			if running {
				close(cm.quit)
				cm.wg.Wait()
				log.Printf("All ceph bg fibers stopped")
				running = false
			}

			if cfg.running {
				if running {
					log.Printf("Attempt to start already running monitor")
					cfg.done <- struct{}{}
					return
				}

				cm.cluster = cfg.cluster
				cm.osdIDs = make([]int, len(cfg.osdIDS))
				copy(cm.osdIDs, cfg.osdIDS)
				latsListChan := make(chan *cephLats, 1)
				statProxyChan := make(chan *CephStatus, 1)

				cm.quit = make(chan struct{})
				cm.timeout = cfg.timeout

				cm.wg.Add(3)

				// turn off local stubs
				latsHistoChan = nil
				latsHistoChanNoClear = nil
				cephStatChan = nil

				go cm.statusMonitoringFiber(statProxyChan)
				go cm.latencyMonitoringFiber(latsListChan)
				go cm.storageFiber(latsListChan, statProxyChan, cfg.min, cfg.max, cfg.bins)

				log.Printf("Monitoring started, result channel is %v", cm.latsHistoChan)
			} else {
				// switch channels to local stubs
				latsHistoChan = cm.latsHistoChan
				latsHistoChanNoClear = cm.latsHistoChanNoClear
				cephStatChan = cm.cephStatChan
			}
			cfg.done <- struct{}{}
		case latsHistoChan <- nil:
		case latsHistoChanNoClear <- nil:
		case cephStatChan <- nil:
		}
	}
}

func (cm *cephMonitor) config(osdIDS []int, cluster string, timeout int, min, max, bins uint32) {
	done := make(chan struct{})
	cm.monitoringConfigChan <- &monitoringConfig{
		running:true,
		cluster: cluster,
		osdIDS: osdIDS,
		timeout: timeout,
		min: min,
		max: max,
		bins: bins,
		done: done}
	<- done
}

func (cm *cephMonitor) statusMonitoringFiber(statProxyChan chan<- *CephStatus) {
	log.Printf("Status monitoring fiber started")
	defer cm.wg.Done()
	defer close(statProxyChan)
	tk := time.NewTicker(time.Duration(cm.timeout) * time.Second)
	defer tk.Stop()

	for {
		log.Printf("Getting new ceph status")
		stat, _ := getCephStatus(cm.cluster)
		statProxyChan <- stat

		select{
		case <- cm.quit:
			log.Printf("Status monitoring fiber stopped due to quit channel closed")
			return
		case <- tk.C:
		}
	}
}

func (cm *cephMonitor) stop() {
	done := make(chan struct{})
	cm.monitoringConfigChan <- &monitoringConfig{running:false, done: done}
	<- done
}

func (cm *cephMonitor) exit() {
	close(cm.monitoringConfigChan)
}

func (cm *cephMonitor) get() *cephLatsHisto {
	return <-cm.latsHistoChan
}

func (cm *cephMonitor) getNoClear() *cephLatsHisto {
	return <-cm.latsHistoChanNoClear
}

func (cm *cephMonitor) getStatus() (*CephStatus, error) {
	res := <-cm.cephStatChan
	if res == nil {
		return getCephStatus("ceph")
	}
	return res, nil
}

func (cm *cephMonitor) latencyMonitoringFiber(latsChan chan<- *cephLats) {
	defer cm.wg.Done()
	defer close(latsChan)

	tk := time.NewTicker(time.Duration(cm.timeout) * time.Second)
	defer tk.Stop()

	log.Printf("Latency monitoring fiber started")
	prevOpsMap := make([]map[string]bool,  len(cm.osdIDs))
	for idx := range cm.osdIDs {
		prevOpsMap[idx] = make(map[string]bool)
	}

	wgSubl := sync.WaitGroup{}

	for {
		log.Printf("Start collecting lats for osd id %v", cm.osdIDs)
		wgSubl.Add(len(cm.osdIDs))
		for idx, osdID := range cm.osdIDs {
			go func(osdID int, prevOPS *map[string]bool) {
				defer wgSubl.Done()
				lats, err := getLatList(osdID, cm.cluster, prevOPS)
				if err == nil {
					latsChan <- lats
				} else {
					log.Printf("ERROR: Failed to get lat from %d: %v", osdID, err)
				}
			}(osdID, &prevOpsMap[idx])
		}

		wgSubl.Wait()
		select{
		case <- cm.quit:
			log.Print("Latency monitoring fiber stopped due to cm.quit")
			return
		case <- tk.C:
		}
	}
}

func  (cm *cephMonitor) storageFiber(latsListChan <-chan *cephLats, statProxyChan <-chan *CephStatus, min, max, bins uint32) {
	defer cm.wg.Done()

	log.Printf("Storage fiber started")
	rhisto := makeHisto(float64(min), float64(max), int(bins))
	whisto := makeHisto(float64(min), float64(max), int(bins))
	var currStatus *CephStatus
	for statProxyChan != nil || latsListChan != nil {
		select {
		case status, ok := <-statProxyChan:
			if !ok {
				statProxyChan = nil
			} else {
				currStatus = status
			}
		case cm.cephStatChan <- currStatus:
		case newData, ok := <- latsListChan:
			if !ok {
				latsListChan = nil
			} else {
				rhisto.update(newData.rlats)
				whisto.update(newData.wlats)
			}
		case cm.latsHistoChan <- &cephLatsHisto{rhisto.bins, whisto.bins}:
			whisto.clean()
			rhisto.clean()
		case cm.latsHistoChanNoClear <- &cephLatsHisto{rhisto.bins, whisto.bins}:
		}
	}
	log.Printf("Storage fiber stopped due to all input channels closed")
}

func (status *CephStatus) serialize()([]byte, error) {
	return proto.Marshal(status)
}

func parseCephHealth(cephSBt []byte) (*CephStatus, error) {
	cephS, err := parseJSON(cephSBt)
	if err != nil {
		return nil, err
	}
	var status CephStatus

	overallStatus := getJSONFieldStr(cephS, "health", "overall_status")
	switch overallStatus {
	case "HEALTH_OK":
		status.Status = HealthOk;
	case "HEALTH_WARN":
		status.Status = HealthWarn;
	case "HEALTH_ERR":
		status.Status = HealtheErr;
	default:
		log.Fatal("Unknown status", overallStatus)
	}

	GiB := float64(math.Pow(1024, 3))
	status.DataG = uint32(getJSONFieldFloat(cephS, "pgmap", "data_bytes") / GiB)
	status.UsedG = uint32(getJSONFieldFloat(cephS, "pgmap", "bytes_used") / GiB)
	status.FreeG = uint32(getJSONFieldFloat(cephS, "pgmap", "bytes_avail") / GiB)
	status.OsdMapEpoch = uint32(getJSONFieldFloat(cephS, "osdmap", "osdmap", "epoch") + 0.5)

	return &status, nil
}

func getCephStatus(name string) (*CephStatus, error) {
	cmd := exec.Command("ceph", "-s", "--format", "json", "--cluster", name)
	cephSBt, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	status, err := parseCephHealth(cephSBt)
	if err != nil {
		return nil, err
	}

	cmd = exec.Command("ceph", "osd", "perf", "-f", "json", "--cluster", name)
	osdPerf, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	status.OsdLats, err = parseOsdPerf(osdPerf)
	if err != nil {
		return nil, err
	}

	return status, nil
}

func parseOsdPerf(data []byte) ([]uint32, error) {
	cephPerf, err := parseJSON(data)
	if err != nil {
		return nil, err
	}

	infos := getJSONField(cephPerf, "osd_perf_infos").([]interface{})
	res := make([]uint32, len(infos) * 3)

	for idx, osdDataI := range infos {
		osdData := osdDataI.(map[string]interface{})
		res[idx * 3] = uint32(osdData["id"].(float64))
		res[idx * 3 + 1] = uint32(getJSONFieldFloat(osdData, "perf_stats", "apply_latency_ms"))
		res[idx * 3 + 2] = uint32(getJSONFieldFloat(osdData, "perf_stats", "commit_latency_ms"))
	}

	return res, nil
}

func setHistoryParams(osdID int, cluster string, maxOps int, storeTime int) error {
	runOSDSocketCMD(osdID, cluster, "config", "set", "osd_op_history_duration", strconv.Itoa(storeTime))
	runOSDSocketCMD(osdID, cluster, "config", "set", "osd_op_history_size", strconv.Itoa(maxOps))
	return nil
}

func getOSDSocket(osdID int, cluster string) string {
	return fmt.Sprintf("/var/run/ceph/%s-osd.%d.asok", cluster, osdID)
}

func runOSDSocketCMD(osdID int, cluster string, cmd ...string) ([]byte, error) {
	var newCmd []string
	newCmd = append(newCmd, "--admin-daemon", getOSDSocket(osdID, cluster))
	newCmd = append(newCmd, cmd...)
	log.Print("ceph ", strings.Join(newCmd," "))
	cmdObj := exec.Command("ceph", newCmd...)
	return cmdObj.CombinedOutput()
}

func parseCephTime(timeS string) uint64 {
	timeV := strings.Split(timeS, ".")
	if len(timeV) != 2 {
		panic("Broken datetime format")
	}

	tm, err := time.Parse("2006-01-02 15:04:05", timeV[0])
	if nil != err {
		panic(err)
	}

	val, err := strconv.ParseUint(timeV[1], 10, 64)
	if nil != err {
		panic(err)
	}

	vl := (uint64(tm.UnixNano()) / 1000 + val) / 1000
	return vl
}

const (
	readCephOp = iota
	primaryWriteCephOp = iota
	secondaryWriteCephOp = iota
	otherCephOp = iota
)

type cephOP struct {
	tp int;
	doneTimeMS uint64;
	descr string;
}

func parseCephOp(op *map[string]interface{}, prevOPS *map[string]bool) *cephOP {
	descr := (*op)["description"].(string)

	if !strings.HasPrefix(descr, "osd_op") {
		return nil
	}

	// check that this is new op
	_, ok := (*prevOPS)[descr]
	if ok {
		return nil
	}
	// detect op type
	typeData := (*op)["type_data"].([]interface{})
	stages := typeData[2].([]interface{})

	descrV := strings.Split(descr, " ")
	if len(descrV) != 8 {
		return nil
	}

	tp := descrV[6]
	var opTp int
	var endTime uint64
	if strings.HasPrefix(tp, "ack+read+") {
		// read
		opTp = readCephOp
		lastStage := stages[len(stages) - 1].(map[string]interface{})
		endTime = parseCephTime(lastStage["time"].(string))
	} else if strings.HasPrefix(tp, "ondisk+write+"){
		// write
		opTp = primaryWriteCephOp
		for _, stagei := range stages {
			stage := stagei.(map[string]interface{})
			if stage["event"].(string) == "commit_sent" {
				endTime = parseCephTime(stage["time"].(string))
				break
			}
		}
	}

	if endTime == 0 {
		return nil
	}

	dt := endTime - parseCephTime(stages[0].(map[string]interface{})["time"].(string))
	if dt > math.MaxUint32 {
		dt = math.MaxUint32
	}

	return &cephOP{opTp, dt, descr}
}


func getLatList(osdID int, cluster string, prevOPS *map[string]bool) (*cephLats, error) {
	cmd := []string{"dump_historic_ops"}
	out, err := runOSDSocketCMD(osdID, cluster, cmd...)
	if err != nil {
		return nil, err
	}
	histOps, err := parseJSON(out)
	if err != nil {
		return nil, err
	}

	ops := getJSONField(histOps, "Ops").([]interface{})
	rtimes := make([]uint32, 0, len(ops))
	wtimes := make([]uint32, 0, len(ops))
	currOps := make([]string, 0, len(ops))

	for _, opI := range ops {
		opM := opI.(map[string]interface{})
		op := parseCephOp(&opM, prevOPS)
		if op != nil {
			if op.tp == readCephOp {
				rtimes = append(rtimes, uint32(op.doneTimeMS))
			} else if op.tp == primaryWriteCephOp {
				wtimes = append(wtimes, uint32(op.doneTimeMS))
			}
			currOps = append(currOps, op.descr)
		}
	}

	for k := range *prevOPS {
		delete(*prevOPS, k)
	}

	for _, val := range currOps {
		(*prevOPS)[val] = true
	}

	return &cephLats{rtimes, wtimes}, nil
}


func getCephInfo(cluster string) ([]byte, []byte, error) {
	cmd := exec.Command("ceph", "osd", "tree", "--format", "json", "--cluster", cluster)
	osdTree, err := cmd.CombinedOutput()
	if err != nil {
		return nil, nil, err
	}

	cmd = exec.Command("ceph", "osd", "dump", "--format", "json", "--cluster", cluster)
	osdDump, err := cmd.CombinedOutput()
	if err != nil {
		return nil, nil, err
	}
	return  osdTree, osdDump, nil
}


func listOSDs(cluster string) (map[int]string, error) {
	cmd := exec.Command("ceph", "osd", "dump", "--format", "json", "--cluster", cluster)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	osdDump, err := parseJSON(out)
	if err != nil {
		return nil, err
	}

	res := make(map[int]string)
	for _, osdInfoI := range getJSONField(osdDump, "osds").([]interface{}) {
		osdPublicAddr := osdInfoI.(map[string]interface{})["public_addr"].(string)
		osdID := osdInfoI.(map[string]interface{})["osd"].(int)
		res[osdID] = osdPublicAddr
	}
	return res, nil
}

type crushNode struct {
	nodeType, name string
	childrens []*crushNode
}

type crushNodeTmp struct {
	nodeType, name string
	id int
	childrens []int
}

func parseOSDTree(cluster string) ([]*crushNode, error) {
	cmd := exec.Command("ceph", "osd", "tree", "--format", "json", "--cluster", cluster)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	osdDump, err := parseJSON(out)
	if err != nil {
		return nil, err
	}

	allNodes := make(map[int]*crushNodeTmp)

	for _, nodeI := range getJSONField(osdDump, "nodes").([]interface{}) {
		node := nodeI.(map[string]interface{})
		nodeID := node["id"].(int)
		nodeTypeName := node["type"].(string)
		nodeName := node["name"].(string)
		childrenI := node["children"].([]interface{})
		cntmp := crushNodeTmp{nodeType: nodeTypeName, name: nodeName, id: nodeID,
							  childrens: make([]int, len(childrenI))}
		for idx, vl := range childrenI {
			cntmp.childrens[idx] = vl.(int)
		}
		allNodes[cntmp.id] = &cntmp
	}

	resMap := make(map[int]*crushNode)

	for _, nodeTmp := range allNodes {
		node := crushNode{nodeType: nodeTmp.nodeType, name: nodeTmp.name,
						  childrens: make([]*crushNode, len(nodeTmp.childrens))}
		resMap[nodeTmp.id] = &node
	}

	resLst := make([]*crushNode, 0)
	for nodeID, node := range resMap {
		for idx, childID := range allNodes[nodeID].childrens {
			node.childrens[idx] = resMap[childID]
		}
		resLst = append(resLst, node)
	}


	return resLst, nil
}
