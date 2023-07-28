package worker

import (
	"context"
	"github.com/shenzan/hackathon/protocol"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoBatchDAO stores logs in MongoDB in batches
type MongoBatchDAO struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *protocol.JobLog
	autoCommitChan chan *protocol.LogBatch
}

var (
	// Singleton instance
	G_MongoBatchDAO *MongoBatchDAO
)

// saveLogs saves a batch of logs to MongoDB
func (mongoBatchDAO *MongoBatchDAO) saveLogs(batch *protocol.LogBatch) {
	mongoBatchDAO.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// writeLoop is a goroutine for handling log storage
func (mongoBatchDAO *MongoBatchDAO) writeLoop() {
	var (
		log          *protocol.JobLog
		logBatch     *protocol.LogBatch // Current batch
		commitTimer  *time.Timer
		timeoutBatch *protocol.LogBatch // Timeout batch
	)

	for {
		select {
		case log = <-mongoBatchDAO.logChan:
			if logBatch == nil {
				logBatch = &protocol.LogBatch{}
				// Set a timer to auto-commit the batch after a certain duration (1 second)
				commitTimer = time.AfterFunc(
					time.Duration(WorkerConf.JobLogCommitTimeout)*time.Millisecond,
					func(batch *protocol.LogBatch) func() {
						return func() {
							mongoBatchDAO.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}

			// Append the new log to the batch
			logBatch.Logs = append(logBatch.Logs, log)

			// If the batch is full, send it immediately
			if len(logBatch.Logs) >= WorkerConf.JobLogBatchSize {
				// Send the logs
				mongoBatchDAO.saveLogs(logBatch)
				// Clear logBatch
				logBatch = nil
				// Stop the timer
				commitTimer.Stop()
			}
		case timeoutBatch = <-mongoBatchDAO.autoCommitChan: // Expired batch
			// Check if the expired batch is still the current batch
			if timeoutBatch != logBatch {
				continue // Skip the batch that has already been committed
			}
			// Write the batch to MongoDB
			mongoBatchDAO.saveLogs(timeoutBatch)
			// Clear logBatch
			logBatch = nil
		}
	}
}

// InitMongoBatchDAO initializes the MongoDB log storage and starts the storage goroutine
func InitMongoBatchDAO() (err error) {
	var (
		client *mongo.Client
	)

	// Establish a connection to MongoDB
	if client, err = mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(WorkerConf.MongodbUri)); err != nil {
		return
	}

	// Select the database and collection
	G_MongoBatchDAO = &MongoBatchDAO{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *protocol.JobLog, 1000),
		autoCommitChan: make(chan *protocol.LogBatch, 1000),
	}

	// Start a MongoDB processing goroutine
	go G_MongoBatchDAO.writeLoop()
	return
}

// Append sends a log to the logChan for storage
func (mongoBatchDAO *MongoBatchDAO) Append(jobLog *protocol.JobLog) {
	select {
	case mongoBatchDAO.logChan <- jobLog:
	default:
		// If the queue is full, discard the log
	}
}
