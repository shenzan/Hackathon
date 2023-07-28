package worker

import (
	"context"
	"github.com/shenzan/hackathon/protocol"
	"go.etcd.io/etcd/client/v3"
)

type JobLock struct {
	kv        clientv3.KV
	lease     clientv3.Lease
	jobName   string
	cancelFunc context.CancelFunc
	leaseID   clientv3.LeaseID
	isLocked  bool
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) *JobLock {
	return &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
}

func (jobLock *JobLock) TryLock() (err error) {
	leaseGrantResp, err := jobLock.lease.Grant(context.TODO(), 5)
	if err != nil {
		return err
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	leaseID := leaseGrantResp.ID

	keepRespChan, err := jobLock.lease.KeepAlive(cancelCtx, leaseID)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-cancelCtx.Done():
				return
			case keepResp := <-keepRespChan:
				if keepResp == nil {
					return
				}
			}
		}
	}()

	txn := jobLock.kv.Txn(context.Background())
	lockKey := protocol.JOB_LOCK_DIR + jobLock.jobName

	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(lockKey))

	txnResp, err := txn.Commit()
	if err != nil {
		return err
	}

	if !txnResp.Succeeded {
		err = protocol.ERR_LOCK_ALREADY_REQUIRED
		return err
	}

	jobLock.leaseID = leaseID
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return nil
}

func (jobLock *JobLock) Unlock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.Background(), jobLock.leaseID)
		jobLock.isLocked = false
	}
}
