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

type cephLats struct {
	rlats, wlats []uint32
}

type cephLatsHisto struct {
	rlats, wlats []uint32
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


func monitoringFiber(osdIDS []int, cluster string, timeout int, latsChan chan<- *cephLats, quit <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	prevOpsMap := make([]map[string]bool,  len(osdIDS))
	for idx := range osdIDS {
		prevOpsMap[idx] = make(map[string]bool)
	}

	for {
		select{
		case <- quit:
			close(latsChan)
			log.Print("monitoring fiber stopped")
			return
		default:
		}

		log.Printf("Start collecting lats for osd id %v", osdIDS)
		start := time.Now()

		wgSubl := sync.WaitGroup{}
		wgSubl.Add(len(osdIDS))

		for idx, osdID := range osdIDS {
			go func(osdID int, prevOPS *map[string]bool) {
				defer wgSubl.Done()
				lats, err := getLatList(osdID, cluster, prevOPS)
				if err == nil {
					latsChan <- lats
				} else {
					log.Printf("ERROR: Failed to get lat from %d: %v", osdID, err)
				}
			}(osdID, &prevOpsMap[idx])
		}

		wgSubl.Wait()

		end := time.Now()
		elapsed := time.Duration(timeout) * time.Second - end.Sub(start)
		if elapsed > 0 {
			time.Sleep(elapsed)
		}
	}
}

func latStorageFiber(latsListChan <-chan *cephLats, latsHistoChan chan<- *cephLatsHisto, wg *sync.WaitGroup,
	min, max, bins uint32) {
	defer wg.Done()
	rhisto := makeHisto(float64(min), float64(max), int(bins))
	whisto := makeHisto(float64(min), float64(max), int(bins))
	for {
		select {
		case newData := <- latsListChan:
			if newData == nil {
				log.Printf("Stopping storage fiber")
				return
			}
			rhisto.update(newData.rlats)
			whisto.update(newData.wlats)
			log.Printf("Get new lats chunk with len reads=%d, writes=%d", len(newData.rlats), len(newData.wlats))
			//log.Printf("Updated, rhist == %v, whist == %v", rhisto.bins, whisto.bins)
		case latsHistoChan <- &cephLatsHisto{rhisto.bins, whisto.bins}:
			whisto.clean()
			rhisto.clean()
			log.Printf("Data requested, cleanup storage")
		}
	}
}


type latMonitor struct {
	latsHistoChan chan *cephLatsHisto
	quit chan struct{}
	wg sync.WaitGroup
	running bool
}

func newLm() *latMonitor {
	return &latMonitor{
		latsHistoChan: make(chan *cephLatsHisto),
		quit: make(chan struct{}),
		running: false,
	}
}

func (lm *latMonitor) start(osdIDS []int, cluster string, timeout int, min, max, bins uint32) {
	if lm.running {
		log.Fatal("Starting already running monitor")
	}

	latsListChan := make(chan *cephLats)
	go monitoringFiber(osdIDS, cluster, timeout, latsListChan, lm.quit, &lm.wg)
	go latStorageFiber(latsListChan, lm.latsHistoChan, &lm.wg, min, max, bins)
	log.Printf("Monitoring started, result channel is %v", lm.latsHistoChan)
	lm.wg.Add(2)
	lm.running = true
}

func (lm *latMonitor) stop() {
	if lm.running {
		lm.quit <- struct{}{}
		lm.wg.Wait()
		lm.running = false
	}
}

func (lm *latMonitor) get() *cephLatsHisto {
	if !lm.running {
		return nil
	}
	return <-lm.latsHistoChan
}

func (lm *latMonitor) reconfig(osdIDS []int, cluster string, timeout int, min, max, bins uint32) {
	lm.stop()
	lm.start(osdIDS, cluster, timeout, min, max, bins)
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
