package main

import ("math"
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
	exit bool
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
	monitoringConfigChan chan<- *monitoringConfig
	wg sync.WaitGroup
	timeout int
}


func newCephMonitor() *cephMonitor {
	cch := make(chan *monitoringConfig)
	cm := &cephMonitor{
		latsHistoChan: make(chan *cephLatsHisto),
		latsHistoChanNoClear: make(chan *cephLatsHisto),
		cephStatChan: make(chan *CephStatus),
		monitoringConfigChan: cch,
		osdIDs: make([]int, 0),
	}
	go cm.configFiber(cch)
	return cm
}


func (cm *cephMonitor) configFiber(configChannel <-chan *monitoringConfig) {
	clog.Info("Ceph config fiber started")

	// start with stubs
	latsHistoChan := cm.latsHistoChan
	latsHistoChanNoClear := cm.latsHistoChanNoClear
	cephStatChan := cm.cephStatChan
	var quit chan struct{}

	exit := false
	for {
		select {
		case cfg := <-configChannel:
			// stop routines
			if quit != nil {
				close(quit)
				cm.wg.Wait()
				clog.Info("All ceph bg fibers stopped")
				quit = nil
			}

			if cfg != nil {
				exit = cfg.exit
			}

			if cfg == nil || exit {
				close(cm.latsHistoChan)
				close(cm.latsHistoChanNoClear)
				close(cm.cephStatChan)
				if cfg != nil {
					defer func() { cfg.done <- struct{}{} }()
				}
				clog.Info("Ceph config fiber exits due to request")
				return
			}

			// start routines
			if cfg.running {
				if quit != nil {
					clog.Warn("Attempt to start already running monitor")
					cfg.done <- struct{}{}
					continue
				}

				// turn off local stubs
				latsHistoChan = nil
				latsHistoChanNoClear = nil
				cephStatChan = nil

				cm.cluster = cfg.cluster
				cm.osdIDs = make([]int, len(cfg.osdIDS))
				cm.timeout = cfg.timeout
				copy(cm.osdIDs, cfg.osdIDS)

				latsListChan := make(chan *cephLats)
				statProxyChan := make(chan *CephStatus)
				quit = make(chan struct{})

				cm.wg.Add(3)
				go cm.statusMonitoringFiber(statProxyChan, quit)
				go cm.latencyMonitoringFiber(latsListChan, quit)
				go cm.storageFiber(latsListChan, statProxyChan, cfg.min, cfg.max, cfg.bins)

				clog.Debugf("Monitoring started, result channel is %v", cm.latsHistoChan)
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

func (cm *cephMonitor) statusMonitoringFiber(statProxyChan chan<- *CephStatus, quit <-chan struct{}) {
	clog.Info("Status monitoring fiber started")
	defer cm.wg.Done()
	defer close(statProxyChan)
	tk := time.NewTicker(time.Duration(cm.timeout) * time.Second)
	defer tk.Stop()

	for {
		clog.Debug("Getting new ceph status")
		stat, _ := getCephStatus(cm.cluster)
		statProxyChan <- stat

		select{
		case <- quit:
			clog.Info("Status monitoring fiber stopped due to quit channel closed")
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
	done := make(chan struct{})
	cm.monitoringConfigChan <- &monitoringConfig{exit: true, done: done}
	<- done
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

func (cm *cephMonitor) latencyMonitoringFiber(latsChan chan<- *cephLats, quit <-chan struct{}) {
	defer cm.wg.Done()
	defer close(latsChan)

	tk := time.NewTicker(time.Duration(cm.timeout) * time.Second)
	defer tk.Stop()

	clog.Info("Latency monitoring fiber started")
	prevOpsMap := make([]map[string]bool,  len(cm.osdIDs))
	for idx := range cm.osdIDs {
		prevOpsMap[idx] = make(map[string]bool)
	}

	wgSubl := sync.WaitGroup{}

	for {
		clog.Debug("Start collecting lats for osd id %v", cm.osdIDs)
		wgSubl.Add(len(cm.osdIDs))
		for idx, osdID := range cm.osdIDs {
			go func(osdID int, prevOPS *map[string]bool) {
				defer wgSubl.Done()
				lats, err := getLatList(osdID, cm.cluster, prevOPS)
				if err == nil {
					latsChan <- lats
				} else {
					clog.Errorf("ERROR: Failed to get lat from %d: %v", osdID, err)
				}
			}(osdID, &prevOpsMap[idx])
		}

		wgSubl.Wait()
		select{
		case <- quit:
			clog.Info("Latency monitoring fiber stopped due to cm.quit")
			return
		case <- tk.C:
		}
	}
}

func  (cm *cephMonitor) storageFiber(latsListChan <-chan *cephLats,
									 statProxyChan <-chan *CephStatus, min, max, bins uint32) {
	defer cm.wg.Done()

	clog.Info("Storage fiber started")
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
	clog.Info("Storage fiber stopped due to all input channels closed")
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
		clog.Panic("Unknown status", overallStatus)
		panic("Unknown status")
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
	clog.Debug("ceph ", strings.Join(newCmd," "))
	cmdObj := exec.Command("ceph", newCmd...)
	return cmdObj.CombinedOutput()
}

func parseCephTime(timeS string) uint64 {
	timeV := strings.Split(timeS, ".")
	if len(timeV) != 2 {
		clog.Panic("Broken datetime format")
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
