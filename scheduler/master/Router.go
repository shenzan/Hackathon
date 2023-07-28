package master

import (
	"encoding/json"
	"fmt"
	"github.com/shenzan/hackathon/protocol"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

func NewApiServer() *ApiServer {
	return &ApiServer{}
}

func (server *ApiServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/job/save", errorHandler(handleJobSave))
	mux.HandleFunc("/job/delete", errorHandler(handleJobDelete))
	mux.HandleFunc("/job/list", errorHandler(handleJobList))
	mux.HandleFunc("/job/kill", errorHandler(handleJobKill))
	mux.HandleFunc("/job/log", errorHandler(handleJobLog))
	mux.HandleFunc("/worker/list", errorHandler(handleWorkerList))

	pwd, _ := os.Getwd()
	path := filepath.Join(pwd, Master_conf.WebRoot)
	_, err := os.Stat(path)
	if err != nil {
		path = filepath.Join(pwd, "main", Master_conf.WebRoot)
	}
	staticDir := http.Dir(path)
	fmt.Println("staticDir:", staticDir)


	staticHandler := http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(Master_conf.ApiPort))
	if err != nil {
		return err
	}

	server.httpServer = &http.Server{
		ReadTimeout:  time.Duration(Master_conf.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(Master_conf.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	go server.httpServer.Serve(listener)

	return nil
}

func errorHandler(handler http.HandlerFunc) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered from panic:", r)
				http.Error(resp, "Internal Server Error", http.StatusInternalServerError)
			}
		}()

		handler(resp, req)
	}
}

type ApiResponse struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

func respondWithError(resp http.ResponseWriter, statusCode int, err error) {
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(statusCode)

	apiResp := ApiResponse{
		Errno: -1,
		Msg:   err.Error(),
		Data:  nil,
	}

	json.NewEncoder(resp).Encode(apiResp)
}

func respondWithJSON(resp http.ResponseWriter, statusCode int, data interface{}) {
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(statusCode)

	apiResp := ApiResponse{
		Errno: 0,
		Msg:   "success",
		Data:  data,
	}

	json.NewEncoder(resp).Encode(apiResp)
}

func handleJobSave(resp http.ResponseWriter, req *http.Request) {
	var (
		err     error
		postJob string
		job     protocol.Job
		oldJob  *protocol.Job
	)

	if err = req.ParseForm(); err != nil {
		respondWithError(resp, http.StatusBadRequest, err)
		return
	}

	postJob = req.PostForm.Get("job")

	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		respondWithError(resp, http.StatusBadRequest, err)
		return
	}

	oldJob, err = G_JobManager.SaveJob(&job)
	if err != nil {
		respondWithError(resp, http.StatusInternalServerError, err)
		return
	}

	respondWithJSON(resp, http.StatusOK, oldJob)
}

func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var (
		err    error
		name   string
		oldJob *protocol.Job
	)

	if err = req.ParseForm(); err != nil {
		respondWithError(resp, http.StatusBadRequest, err)
		return
	}

	name = req.PostForm.Get("name")

	oldJob, err = G_JobManager.DeleteJob(name)
	if err != nil {
		respondWithError(resp, http.StatusInternalServerError, err)
		return
	}

	respondWithJSON(resp, http.StatusOK, oldJob)
}

func handleJobList(resp http.ResponseWriter, req *http.Request) {
	jobList, err := G_JobManager.ListJobs()
	if err != nil {
		respondWithError(resp, http.StatusInternalServerError, err)
		return
	}

	respondWithJSON(resp, http.StatusOK, jobList)
}

func handleJobKill(resp http.ResponseWriter, req *http.Request) {
	var (
		err  error
		name string
	)

	if err = req.ParseForm(); err != nil {
		respondWithError(resp, http.StatusBadRequest, err)
		return
	}

	name = req.PostForm.Get("name")

	err = G_JobManager.KillJob(name)
	if err != nil {
		respondWithError(resp, http.StatusInternalServerError, err)
		return
	}

	respondWithJSON(resp, http.StatusOK, nil)
}

func handleJobLog(resp http.ResponseWriter, req *http.Request) {
	var (
		err  error
		name string
		logArr []*protocol.JobLog
	)

	if err = req.ParseForm(); err != nil {
		respondWithError(resp, http.StatusBadRequest, err)
		return
	}

	name = req.Form.Get("name")
	logArr, err = G_MongoDAO.ListLog(name, 0, 20)
	if err != nil {
		respondWithError(resp, http.StatusInternalServerError, err)
		return
	}

	respondWithJSON(resp, http.StatusOK, logArr)
}

func handleWorkerList(resp http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err       error
	)

	// Retrieve the list of workers
	if workerArr, err = G_WorkerManager.ListWorkers(); err != nil {
		respondWithError(resp, http.StatusInternalServerError, err)
		return
	}

	// Respond with the worker list
	respondWithJSON(resp, http.StatusOK, workerArr)
}