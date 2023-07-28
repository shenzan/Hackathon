package master

import (
	"context"
	"github.com/shenzan/hackathon/protocol"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDAO struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_MongoDAO *MongoDAO
)

func InitLogMgr() (err error) {
	clientOptions := options.Client().ApplyURI(Master_conf.MongodbUri)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}

	G_MongoDAO = &MongoDAO{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return nil
}

func (logMgr *MongoDAO) ListLog(name string, skip int, limit int) (logArr []*protocol.JobLog, err error) {
	logArr = make([]*protocol.JobLog, 0)

	filter := &protocol.JobLogFilter{JobName: name}

	opts := options.Find().SetSort(map[string]int{"startTime": -1}).SetSkip(int64(skip)).SetLimit(int64(limit))

	cursor, err := logMgr.logCollection.Find(context.TODO(), filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog := &protocol.JobLog{}
		if err = cursor.Decode(jobLog); err != nil {
			continue
		}
		logArr = append(logArr, jobLog)
	}
	return logArr, nil
}
