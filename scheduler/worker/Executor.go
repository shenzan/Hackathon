package worker

import (
	"github.com/shenzan/hackathon/protocol"
	"math/rand"
	"os/exec"
	"time"
)

type Executor struct {
	localIP string
}

var (
	G_executor *Executor
)

func (executor *Executor) ExecuteJob(info *protocol.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *protocol.JobExecuteResult
			jobLock *JobLock
		)

		result = &protocol.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
			LocalIP:    executor.localIP,
		}

		jobLock = G_JobWatcher.CreateJobLock(info.Job.Name)

		result.StartTime = time.Now()

		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil { // 上锁失败
			result.Err = err
			result.EndTime = time.Now()
		} else {
			// 上锁成功后，重置任务启动时间
			result.StartTime = time.Now()

			// 执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "powershell", "-Command", info.Job.Command)

			// 执行并捕获输出
			output, err = cmd.CombinedOutput()

			// 记录任务结束时间
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}
		// 任务执行完成后，把执行的结果返回给Scheduler，Scheduler会从executingTable中删除掉执行记录
		G_scheduler.PushJobResult(result)
	}()
}

func InitExecutor(localIP string) (err error) {
	G_executor = &Executor{localIP}
	return
}
