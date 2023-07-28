package master

import (
	"context"
	"encoding/json"
	"github.com/shenzan/hackathon/protocol"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"time"
)

type JobManager struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_JobManager    *JobManager
	G_WorkerManager *WorkerManager
)

func NewJobManager(endpoints []string, dialTimeout time.Duration) (*JobManager, error) {
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	return &JobManager{
		client: client,
		kv:     kv,
		lease:  lease,
	}, nil
}

func (jobMgr *JobManager) SaveJob(job *protocol.Job) (oldJob *protocol.Job, err error) {
	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj protocol.Job
	)

	jobKey = protocol.JOB_SAVE_DIR + job.Name

	if jobValue, err = json.Marshal(job); err != nil {
		return
	}

	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}

	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}


func (jobMgr *JobManager) DeleteJob(name string) (oldJob *protocol.Job, err error) {
	var (
		jobKey    string
		delResp   *clientv3.DeleteResponse
		oldJobObj protocol.Job
	)

	jobKey = protocol.JOB_SAVE_DIR + name

	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	if len(delResp.PrevKvs) != 0 {
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

func (jobMgr *JobManager) ListJobs() (jobList []*protocol.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *protocol.Job
	)

	dirKey = protocol.JOB_SAVE_DIR

	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	jobList = make([]*protocol.Job, 0)

	for _, kvPair = range getResp.Kvs {
		job = &protocol.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

func (jobMgr *JobManager) KillJob(name string) (err error) {
	var (
		killerKey      string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
	)

	killerKey = protocol.JOB_KILLER_DIR + name

	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	leaseId = leaseGrantResp.ID

	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}

// /cron/workers/
type WorkerManager struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

func (workerMgr *WorkerManager) ListWorkers() (workerArr []string, err error) {
	var (
		getResp  *clientv3.GetResponse
		kv       *mvccpb.KeyValue
		workerIP string
	)

	workerArr = make([]string, 0)

	if getResp, err = workerMgr.kv.Get(context.TODO(), protocol.JOB_WORKER_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kv = range getResp.Kvs {
		// kv.Key : /cron/workers/192.168.2.1
		workerIP = protocol.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}
	return
}

func NewWorkerManager(endpoints []string, dialTimeout time.Duration) (*WorkerManager, error) {
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	return &WorkerManager{
		client: client,
		kv:     kv,
		lease:  lease,
	}, nil
}

