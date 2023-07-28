package main

import (
	"flag"
	"github.com/shenzan/hackathon/worker"
	"log"
	"runtime"
	"time"
)

var (
	confFile string
	localIP  string
)

func initArgs() {
	flag.StringVar(&confFile, "config", "./config.json", "worker.json")
	flag.StringVar(&localIP, "localIP", "0.0.0.0", "for test")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	initArgs()

	initEnv()

	if err := worker.InitConfig(confFile); err != nil {
		log.Fatal(err)
	}

	if err := worker.InitRegister(localIP); err != nil {
		log.Fatal(err)
	}

	if err := worker.InitMongoBatchDAO(); err != nil {
		log.Fatal(err)
	}

	if err := worker.InitExecutor(localIP); err != nil {
		log.Fatal(err)
	}

	if err := worker.InitScheduler(); err != nil {
		log.Fatal(err)
	}

	if err := worker.InitJobMgr(); err != nil {
		log.Fatal(err)
	}

	for {
		time.Sleep(1 * time.Second)
	}
}
