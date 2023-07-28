package worker

import (
	"context"
	"github.com/shenzan/hackathon/protocol"
	"go.etcd.io/etcd/client/v3"
	"time"
)

// Register is responsible for registering the worker node to etcd: /cron/workers/IP-address
type Register struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	localIP string // Local IP address
}

var (
	G_register *Register
)

// keepOnline registers the worker node to /cron/workers/IP-address and keeps it online by renewing the lease
func (register *Register) keepOnline() {
	var (
		regKey         string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err            error
		keepAliveChan  <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp  *clientv3.LeaseKeepAliveResponse
		cancelCtx      context.Context
		cancelFunc     context.CancelFunc
	)

	for {
		// Registration path
		regKey = protocol.JOB_WORKER_DIR + register.localIP

		cancelFunc = nil

		// Create a lease
		if leaseGrantResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		// Start renewing the lease
		if keepAliveChan, err = register.lease.KeepAlive(context.TODO(), leaseGrantResp.ID); err != nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.Background())

		// Register the worker node to etcd
		if _, err = register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			goto RETRY
		}

		// Handle lease renewal response
		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				if keepAliveResp == nil { // Lease renewal failed
					goto RETRY
				}
			}
		}

	RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}

// InitRegister initializes the worker node registration and keeps it online in etcd
func InitRegister(localIP string) (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	// Initialize the configuration
	config = clientv3.Config{
		Endpoints:   WorkerConf.EtcdEndpoints,                                     // Cluster addresses
		DialTimeout: time.Duration(WorkerConf.EtcdDialTimeout) * time.Millisecond, // Connection timeout
	}

	// Establish a connection
	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIP: localIP,
	}

	go G_register.keepOnline()

	return
}
