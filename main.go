package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	md := os.Getenv("mode")

	cfg := Ucpconfig{
		port:   6668,
		wakeup: true,
		ip:     "4.4.4.56",
	}
	fmt.Println("md ", md)
	if md == "server" {
		fmt.Println("mode = server")
		for i := 0; i < 1; i++ {
			go func() {
				serverstart(cfg)
			}()
			time.Sleep(time.Second)
			cfg.port++
		}
		select {}

	} else {
		fmt.Println("mode = client")
		for i := 0; i < 1; i++ {
			go func() {
				clientstart(cfg)
			}()
			time.Sleep(time.Second)
			cfg.port++
		}

		select {}
	}

}

func serverstart(cfg Ucpconfig) {
	NewServer(cfg)
}

func clientstart(cfg Ucpconfig) {

	cli := NewClient(cfg)
	err := cli.Connect(cfg)
	if err != nil {
		log.Println(err)
	}
	cli.progressWorker(cfg)

	start := time.Now()
	cli.sendinfo(0, []byte("666"))
	//fmt.Println("res")
	size := uint64(0)
	revcnt := 0
	lastsize := uint64(0)

	for res := range cli.reschan {
		size += res
		lastsize = res
		revcnt++
		//delay to sim the data process cost
		//time.Sleep(time.Millisecond * 10)
		cli.sendinfo(0, []byte("need more"))
	}
	fmt.Println("receive time ", revcnt, "last size ", lastsize)
	delta := time.Since(start).Seconds()
	M := float64(size) / 1024 / 1024 / delta
	fmt.Println("file ", " ", M, "MB/sec", " cost", time.Since(start).String())
	fmt.Println("file size", (size))
	select {}
	cli.close()

}
