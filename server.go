package main

import (
	. "../../ucx"
	"fmt"
	"net"
	"sync/atomic"
)

type Ucpconfig struct {
	port   uint
	wakeup bool
	ip     string
}

type BstUcxServer struct {
	context      *UcpContext
	listenworker *UcpWorker
	listener     *UcpListener
	workernum    int64
}

func NewServer(cfg Ucpconfig) BstUcxServer {
	var pd BstUcxServer
	pd.initContext(cfg)
	pd.newlistenWorker(cfg)
	pd.initListener(cfg)

	return pd
}

func (bserver *BstUcxServer) newlistenWorker(cfg Ucpconfig) {
	workerParams := (&UcpWorkerParams{}).SetThreadMode(UCS_THREAD_MODE_SINGLE)

	if cfg.wakeup {
		workerParams.WakeupTX()
		workerParams.WakeupRX()
	}

	bserver.listenworker, _ = bserver.context.NewWorker(workerParams)
}

func (bserver *BstUcxServer) newWorker(cfg Ucpconfig) (*BstUcxWorker, error) {
	workerParams := (&UcpWorkerParams{}).SetThreadMode(UCS_THREAD_MODE_SINGLE)

	if cfg.wakeup {
		workerParams.WakeupTX()
		workerParams.WakeupRX()
	}

	newworker, err := bserver.context.NewWorker(workerParams)
	if err != nil {
		return nil, err
	}
	var work BstUcxWorker
	work.UcpWorker = newworker
	work.close = make(chan struct{}, 4)
	go func() {
		work.ReleaseResource()
		atomic.AddInt64(&bserver.workernum, -1)
	}()

	return &work, nil
}

func (bserver *BstUcxServer) progressWorker(cfg Ucpconfig) {
	go func() {
		for {
			for bserver.listenworker.Progress() != 0 {
			}
			if cfg.wakeup {
				bserver.listenworker.Wait()
			}
		}
	}()

}

func (bserver *BstUcxServer) initListener(ucfg Ucpconfig) error {
	var err error
	listenerParams := &UcpListenerParams{}
	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("0.0.0.0:%v", ucfg.port))

	listenerParams.SetSocketAddress(addr)
	listenerParams.SetConnectionHandler(func(connRequest *UcpConnectionRequest) {
		// No need to synchronize, since reverse eps creating from a single thread.
		w, err := bserver.newWorker(ucfg)
		atomic.AddInt64(&bserver.workernum, 1)
		if err != nil {
			fmt.Println("err", err)
			return
		}
		w.SetAmRecvHandler(0, UCP_AM_FLAG_WHOLE_MSG, w.serverAmRecvHandler)
		w.ep, _ = w.NewEndpoint(
			(&UcpEpParams{}).SetConnRequest(connRequest).SetErrorHandler(w.epErrorHandling).SetPeerErrorHandling())
		w.progressWorker(ucfg)
		fmt.Printf("Got connection. Starting creating new worker %d \n", atomic.LoadInt64(&bserver.workernum))
	})

	bserver.listener, err = bserver.listenworker.NewListener(listenerParams)
	if err != nil {
		return err
	}
	bserver.progressWorker(ucfg)

	fmt.Printf("Started receiver listener on address: %v \n", addr)

	return nil
}
func (bserver *BstUcxServer) initContext(ucp Ucpconfig) {
	params := (&UcpParams{}).EnableAM()

	if ucp.wakeup {
		params.EnableWakeup()
	}

	bserver.context, _ = NewUcpContext(params)
}

func (s *BstUcxServer) close() {
	if s.listener != nil {
		s.listener.Close()
	}
	if s.context != nil {
		s.context.Close()
	}
	if s.listenworker != nil {
		s.listenworker.Close()
	}
}
