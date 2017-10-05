package main

import (
	"net"
	"log"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
)

type myRPCServer struct {
	osdIds             []int
	clusterName        string
	historySize        uint32
	historyTime        uint32
	latsCollectTimeout int
	latsMonitor        *latMonitor
}

func (rpc *myRPCServer) init(lm *latMonitor) {
	rpc.latsMonitor = lm
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

	rpc.latsMonitor.reconfig(rpc.osdIds, rpc.clusterName, rpc.latsCollectTimeout,
							 sett.HistoMin, sett.HistoMax, sett.HistoBins)
	return &Empty{}, nil
}


func (rpc *myRPCServer) StopLatencyMonitoring(context.Context, *Empty) (*Empty, error) {
	if rpc.latsMonitor != nil {
		log.Print("Stopping previous monitoring fibers")
		rpc.latsMonitor.stop()
		rpc.latsMonitor = nil
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
	vls := rpc.latsMonitor.get()
	histo := make([]uint32, 0, 0)

	rstart, rstop := getNonzeroReg(vls.rlats)
	wstart, wstop := getNonzeroReg(vls.wlats)

	histo = append(histo, vls.rlats[rstart: rstop]...)
	histo = append(histo, vls.wlats[wstart: wstop]...)

	log.Printf("Len(histo) = %d, len(wlats) = %d, len(rlats) = %d", len(histo), rstop - rstart, wstop - wstart)
	return &CephOpsLats{uint32(rstop - rstart),uint32(rstart), uint32(wstart), histo}, nil
}

func newRPCServer(lm *latMonitor) *myRPCServer {
	mrpc := &myRPCServer{}
	mrpc.init(lm)
	return mrpc
}

func rpcServer(bindTo string, lm *latMonitor) {
	ssok, err := net.Listen("tcp", bindTo)
	if err != nil {
		log.Fatal("Error in net.Listen: ", bindTo, err)
	}
	defer ssok.Close()
	log.Print("Listening on ", bindTo)

	grpcServer := grpc.NewServer()
	mrpc := newRPCServer(lm)
	RegisterSensorRPCServer(grpcServer, mrpc)
	grpcServer.Serve(ssok)
}
