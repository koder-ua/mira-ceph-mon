package main

import (
	"net"
	"log"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"bytes"
	"compress/gzip"
)

type myRPCServer struct {
	osdIds             []int
	clusterName        string
	historySize        uint32
	historyTime        uint32
	latsCollectTimeout int
	cm                 *cephMonitor
}

func (rpc *myRPCServer) init(cm *cephMonitor) {
	rpc.cm = cm
}

func (rpc *myRPCServer) SetupLatencyMonitoring(ctx context.Context, sett *CephSettings) (*Empty, error) {
	log.Printf("Get new config %v", *sett)
	rpc.clusterName = sett.Cluster
	rpc.osdIds = make([]int, len(sett.OsdIDS))
	for idx, vl := range sett.OsdIDS {
		rpc.osdIds[idx] = int(vl)
	}
	rpc.historySize = sett.HistorySize
	rpc.historyTime = sett.HistoryTime
	rpc.latsCollectTimeout = int(sett.LatsCollectTimeout)

	for _, osdID := range rpc.osdIds {
		err := setHistoryParams(osdID, rpc.clusterName, int(rpc.historySize), int(rpc.historyTime))
		if nil != err {
			panic(err)
		}
	}

	rpc.cm.reconfig(rpc.osdIds, rpc.clusterName, rpc.latsCollectTimeout,
		            sett.HistoMin, sett.HistoMax, sett.HistoBins)
	return &Empty{}, nil
}


func (rpc *myRPCServer) StopLatencyMonitoring(context.Context, *Empty) (*Empty, error) {
	if rpc.cm != nil {
		log.Print("Stopping previous monitoring fibers")
		rpc.cm.stop()
		rpc.cm = nil
	}
	return &Empty{}, nil
}

func (rpc *myRPCServer) GetCephStatus(context.Context, *Empty) (*CephStatus, error) {
	return getCephStatus(rpc.clusterName)
}

func getNonzeroReg(arr []uint32) (int, int) {
	la := len(arr)
	firstIdx := la
	lastIdx := la

	for idx, vl := range arr {
		if vl != 0 {
			firstIdx = idx
			break
		}
	}

	for idx := range arr {
		if arr[la - idx - 1] != 0 {
			lastIdx = la - idx
			break
		}
	}

	return firstIdx, lastIdx
}

func (rpc *myRPCServer) GetCephOpsLats(context.Context, *Empty) (*CephOpsLats, error) {
	vls := rpc.cm.get()
	histo := make([]uint32, 0, 0)

	rstart, rstop := getNonzeroReg(vls.rlats)
	wstart, wstop := getNonzeroReg(vls.wlats)

	histo = append(histo, vls.rlats[rstart: rstop]...)
	histo = append(histo, vls.wlats[wstart: wstop]...)

	return &CephOpsLats{uint32(rstop - rstart),uint32(rstart), uint32(wstart), histo}, nil
}


func compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Flush(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (rpc *myRPCServer) GetCephInfo(ctx context.Context, clName *ClusterName) (*CephInfo, error) {
	tree, dump, err := getCephInfo(clName.Name)
	if err != nil {
		return nil, err
	}

	dumpz, err := compress(dump)
	if err != nil {
		return nil, err
	}

	treez, err := compress(tree)
	if err != nil {
		return nil, err
	}

	return &CephInfo{Compressed: true, OsdDump: dumpz, OsdTree: treez}, nil
}

func newRPCServer(cm *cephMonitor) *myRPCServer {
	mrpc := &myRPCServer{}
	mrpc.init(cm)
	return mrpc
}

func rpcServer(bindTo string, cm *cephMonitor) {
	ssok, err := net.Listen("tcp", bindTo)
	if err != nil {
		log.Fatal("Error in net.Listen: ", bindTo, err)
	}
	defer ssok.Close()
	log.Print("Listening on ", bindTo)

	grpcServer := grpc.NewServer()
	mrpc := newRPCServer(cm)
	RegisterSensorRPCServer(grpcServer, mrpc)
	grpcServer.Serve(ssok)
}
