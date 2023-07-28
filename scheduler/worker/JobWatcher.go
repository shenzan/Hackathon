package worker

import (
	"context"
	"github.com/shenzan/hackathon/protocol"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"time"
)

// JobWatcher is responsible for watching job changes and kill events
type JobWatcher struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// Singleton instance
	G_JobWatcher *JobWatcher
)

// watchJobs watches for changes in jobs and syncs them to the scheduler
func (jobMgr *JobWatcher) watchJobs() (err error) {
	var (
		getResp            *clientv3.GetResponse
		kvpair             *mvccpb.KeyValue
		job                *protocol.Job
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobName            string
		jobEvent           *protocol.JobEvent
	)

	// Get all the current jobs under /cron/jobs/ and find out the current cluster's revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), protocol.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	// Process current existing jobs
	for _, kvpair = range getResp.Kvs {
		// Deserialize JSON to get Job
		if job, err = protocol.UnpackJob(kvpair.Value); err == nil {
			jobEvent = protocol.BuildJobEvent(protocol.JOB_EVENT_SAVE, job)
			// Sync the event to the scheduler
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	// Watch for future changes from the current revision onwards
	go func() { // Watch goroutine
		// Start watching from the revision after the GET operation
		watchStartRevision = getResp.Header.Revision + 1
		// Watch for changes in /cron/jobs/ directory
		watchChan = jobMgr.watcher.Watch(context.TODO(), protocol.JOB_SAVE_DIR,
			clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		// Handle watch events
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // Job save event
					if job, err = protocol.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					// Build an update event
					jobEvent = protocol.BuildJobEvent(protocol.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE: // Job deleted
					// Delete /cron/jobs/job10
					jobName = protocol.ExtractJobName(string(watchEvent.Kv.Key))

					job = &protocol.Job{Name: jobName}

					// Build a delete event
					jobEvent = protocol.BuildJobEvent(protocol.JOB_EVENT_DELETE, job)
				}
				// Notify scheduler about the change
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}

// watchKiller watches for kill events and sends them to the scheduler
func (jobMgr *JobWatcher) watchKiller() {
	var (
		watchChan  clientv3.WatchChan
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent   *protocol.JobEvent
		jobName    string
		job        *protocol.Job
	)
	// Watch /cron/killer directory
	go func() { // Watch goroutine
		// Watch for changes in /cron/killer/ directory
		watchChan = jobMgr.watcher.Watch(context.TODO(), protocol.JOB_KILLER_DIR,
			clientv3.WithPrefix())
		// Handle watch events
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // Kill job event
					jobName = protocol.ExtractKillerName(string(watchEvent.Kv.Key))
					job = &protocol.Job{Name: jobName}
					jobEvent = protocol.BuildJobEvent(protocol.JOB_EVENT_KILL, job)
					// Notify scheduler about the event
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: // Killer marker expired and automatically deleted
				}
			}
		}
	}()
}

// InitJobMgr initializes the job manager and starts watching for job changes
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)

	// Initialize the configuration
	config = clientv3.Config{
		Endpoints:   WorkerConf.EtcdEndpoints,                                     // Cluster endpoints
		DialTimeout: time.Duration(WorkerConf.EtcdDialTimeout) * time.Millisecond, // Connection timeout
	}

	// Establish a connection
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// Get KV and Lease API sub-sets
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	// Assign the singleton instance
	G_JobWatcher = &JobWatcher{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	// Start watching for job changes
	G_JobWatcher.watchJobs()

	// Start watching for killer events
	G_JobWatcher.watchKiller()

	return
}

// CreateJobLock creates a job lock for the specified job name
func (jobMgr *JobWatcher) CreateJobLock(jobName string) (jobLock *JobLock) {
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}
