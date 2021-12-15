package main

import (
	. "../../ucx"
	"fmt"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
)


type sig struct {
	p unsafe.Pointer
	size uint64
	end bool

	head unsafe.Pointer
	headsize uint64
}
type BstUcxWorker struct {
	*UcpWorker
	close chan struct{}
	closed int32
	ep    *UcpEp
	first bool
	sendchan chan sig
	context    *UcpContext
	memory               *UcpMemory
	memAttr          *UcpMemAttributes

}

func (w *BstUcxWorker) ReleaseResource() {
	select {
	case <-w.close:
		time.Sleep(time.Second *5)
		if w.ep != nil {
			w.ep.CloseNonBlockingForce(nil)
		}
		w.Close()
		//fmt.Println("worker Release")
	}
}

func (w *BstUcxWorker) epErrorHandling(ep *UcpEp, status UcsStatus) {
	if status != UCS_ERR_CONNECTION_RESET {
		fmt.Printf(" BstUcxWorker Endpoint error: %v \n", status.String())
	}
	atomic.AddInt32(&w.closed,1)
	close(w.close)
}
func (w *BstUcxWorker) progressWorker(cfg Ucpconfig) {
	go func() {
		for {
			select {
			case <-w.close:
				return
			default:
				for w.Progress() != 0 {
				}
				if cfg.wakeup {
					w.Wait()
				}
			}
		}
	}()
}



func (s *BstUcxWorker) serverAmRecvHandler(header unsafe.Pointer, headerSize uint64, data *UcpAmData, replyEp *UcpEp) UcsStatus {

	_ = string(GoBytes(header, headerSize))
	if replyEp == nil {
		fmt.Println("Reply endpoint is not set")
	}else {

		var  chunck int64= 4096000
		if s.first == false{
			s.first = true
			s.sendchan = make(chan sig)


			go func() {
				var aaa string= "im your father"
				head := CBytes([]byte(aaa))

				var data2 []byte = make([]byte,chunck)
				fs, _ := os.Open("client.go")
				stst,_ := fs.Stat()
				stst.Size()
				fmt.Println("filesize ",stst.Size())
				needsend := stst.Size()
				var end bool =false
				stringBytes := CBytes([]byte(data2))
				defer FreeNativeMemory((stringBytes))
				tis := 0
				for i := int64(0); i < needsend ;i+=chunck {
					tis++
					if needsend-i < chunck {
						data2 = data2[:needsend-i]
						end = true
						aaa = "end"
						head = CBytes([]byte(aaa))
						fmt.Println(tis, "time all send",needsend-i, "just fake data")
					}
					//fs.Read(data2)
					s.sendchan <- sig{
						head:     head,
						headsize: uint64(len(aaa)),
						p:        stringBytes,
						size:     uint64(len(data2)),
						end:      end,
					}

				}
				fs.Close()
			}()

		}
		datas := <- s.sendchan
		recvRequest, _ :=replyEp.SendAmNonBlocking(3, datas.head, datas.headsize , datas.p, datas.size, UCP_AM_SEND_FLAG_RNDV, nil)
		for recvRequest.GetStatus() == UCS_INPROGRESS {
			for s.Progress() != 0 {
			}
		}
		recvRequest.Close()


	}
	return UCS_OK
}


