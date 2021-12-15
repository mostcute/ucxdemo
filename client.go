package main

import (
	. "../../ucx"
	"fmt"
	"net"
	"sync/atomic"
	"unsafe"
)

type BstUcpClient struct {
	context    *UcpContext
	sendworker *BstUcxWorker
	reschan    chan uint64
	head       unsafe.Pointer
	headsize   uint64
	dataP      unsafe.Pointer
}

func NewClient(cfg Ucpconfig) *BstUcpClient {
	var bclient BstUcpClient
	bclient.initContext(cfg)
	bclient.NewWorker(cfg)
	bclient.reschan = make(chan uint64, 1)

	var aaa string = "helloworld"
	header := CBytes([]byte(aaa))
	bclient.head = header
	bclient.headsize = uint64(len(aaa))
	data := make([]byte, 4096000)
	bclient.dataP = CBytes([]byte(data))
	return &bclient

}
func (bclient *BstUcpClient) initContext(ucp Ucpconfig) {
	params := (&UcpParams{}).EnableAM()

	if ucp.wakeup {
		params.EnableWakeup()
	}

	bclient.context, _ = NewUcpContext(params)
}

func (bclient *BstUcpClient) NewWorker(cfg Ucpconfig) {
	workerParams := (&UcpWorkerParams{}).SetThreadMode(UCS_THREAD_MODE_SINGLE)

	if cfg.wakeup {
		workerParams.WakeupTX()
		workerParams.WakeupRX()
	}

	newworker, err := bclient.context.NewWorker(workerParams)
	if err != nil {
		fmt.Println("err", err)
		return
	}
	var work BstUcxWorker
	work.UcpWorker = newworker
	work.close = make(chan struct{}, 4)

	bclient.sendworker = &work
	go func() {
		work.ReleaseResource()
	}()

	bclient.sendworker.SetAmRecvHandler(3, UCP_AM_FLAG_WHOLE_MSG, bclient.ClientAmRecvHandler)
}
func (bclient *BstUcpClient) Connect(cfg Ucpconfig) error {
	var err error
	epParams := &UcpEpParams{}
	serverAddress, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%v:%v", cfg.ip, cfg.port))
	epParams.SetPeerErrorHandling().SetErrorHandler(bclient.sendworker.epErrorHandling).SetSocketAddress(serverAddress)

	bclient.sendworker.ep, err = bclient.sendworker.NewEndpoint(epParams)
	if err != nil {
		return err
	}

	request, err := bclient.sendworker.ep.FlushNonBlocking(nil)
	if err != nil {
		return err
	}

	for request.GetStatus() == UCS_INPROGRESS {
		bclient.progressWorkerOnce(cfg)
	}
	fmt.Println("connecnt")

	if status := request.GetStatus(); status != UCS_OK {
		return NewUcxError(status)
	}

	request.Close()
	return nil
}

func (bclient *BstUcpClient) progressWorker(cfg Ucpconfig) {
	go func() {
		for {
			select {
			case <-bclient.sendworker.close:
				fmt.Println("stop")
				return
			default:
				for bclient.sendworker.Progress() != 0 {
				}
				if cfg.wakeup {
					bclient.sendworker.Wait()
				}
			}
		}
	}()

}
func (bclient *BstUcpClient) progressWorkerOnce(cfg Ucpconfig) {
	for bclient.sendworker.Progress() != 0 {
	}
	if cfg.wakeup {
		bclient.sendworker.Wait()
	}
}

func (bclient *BstUcpClient) close() {

	if atomic.LoadInt32(&bclient.sendworker.closed) == 0 {
		close(bclient.sendworker.close)
	}
	bclient.context.Close()
}

func (bclient *BstUcpClient) sendinfo(chanid uint, data []byte) {
	var request *UcpRequest

	//var aaa string= "helloworld"
	//header := CBytes([]byte(aaa))
	//data2 := make([]byte,4096000)
	//stringBytes := CBytes([]byte(data2))
	//defer FreeNativeMemory((stringBytes))

	request, _ = bclient.sendworker.ep.SendAmNonBlocking(chanid, bclient.head, bclient.headsize, nil, 0, UCP_AM_SEND_FLAG_REPLY, nil)

	request.Close()
	return
}

func (bclient *BstUcpClient) ClientAmRecvHandler(header unsafe.Pointer, headerSize uint64, data *UcpAmData, replyEp *UcpEp) UcsStatus {

	//if replyEp == nil {
	//	fmt.Println("Reply endpoint is not set")
	//}else {
	//	fmt.Println("Reply endpoint is  set")
	//}

	he := string(GoBytes(header, headerSize))
	if data.IsDataValid() {
		bclient.reschan <- data.Length()
		if he == "end" {
			close(bclient.reschan)
		}

		return UCS_OK

	} else {

		//fmt.Println("data not Valid",data.Length())
		//time.Sleep(time.Second)
		if data.Length() != 4096000 {
			//s:=time.Now()
			data2 := make([]byte, data.Length())
			//fmt.Println("memalloc step1",time.Since(s).String())
			bclient.dataP = CBytes([]byte(data2))
			//fmt.Println("memalloc",time.Since(s).String())
		}

		data.Receive(bclient.dataP, data.Length(),
			(&UcpRequestParams{}).SetMemType(UCS_MEMORY_TYPE_HOST).SetCallback(func(request *UcpRequest, status UcsStatus, length uint64) {
				//fmt.Println("receive success ",length)
				bclient.reschan <- data.Length() //GoBytes(bclient.dataP, data.Length())
				if he == "end" {
					close(bclient.reschan)
				}
				request.Close()
			}))

	}

	return UCS_OK
}
