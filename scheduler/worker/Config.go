package worker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)


type Config struct {
	EtcdEndpoints         []string `json:"etcdEndpoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
	JobLogBatchSize       int      `json:"jobLogBatchSize"`
	JobLogCommitTimeout   int      `json:"jobLogCommitTimeout"`
}

var (
	WorkerConf *Config
)

func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	pwd, _ := os.Getwd()
	fmt.Println("pwd:", pwd)

	f1 := filepath.Join(pwd, filename)
	fmt.Println("path:", f1)
	if content, err = ioutil.ReadFile(f1); err != nil {
		f1 = filepath.Join(pwd, "main", filename)
		fmt.Println("path:", f1)
		if content, err = ioutil.ReadFile(f1); err != nil {
			return
		}
	}

	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	WorkerConf = &conf

	return
}
