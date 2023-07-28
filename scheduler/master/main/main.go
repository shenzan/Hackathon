package main

import (
	"flag"
	"log"
	"runtime"
	"time"

	"github.com/shenzan/hackathon/master"
)

var (
	confFile string // Config file path
)




func initArgs() {
	flag.StringVar(&confFile, "config", "./config.json", "Config file path")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	initArgs()
	initEnv()

	if err := masterInit(); err != nil {
		log.Fatal(err)
	}

	select {} // Keep the main goroutine running
}

func masterInit() error {
	if err := master.InitConfig(confFile); err != nil {
		return err
	}

	if err := master.InitLogMgr(); err != nil {
		return err
	}

	jobManager, err := master.NewJobManager(master.Master_conf.EtcdEndpoints, time.Duration(master.Master_conf.EtcdDialTimeout)*time.Millisecond)
	if err != nil {
		return err
	}
	master.G_JobManager = jobManager;

	workerMgr, err := master.NewWorkerManager(master.Master_conf.EtcdEndpoints, time.Duration(master.Master_conf.EtcdDialTimeout)*time.Millisecond)
	if err != nil {
		return err
	}
	master.G_WorkerManager = workerMgr

	if err := master.NewApiServer().Start(); err != nil {
		return err
	}

	return nil
}






