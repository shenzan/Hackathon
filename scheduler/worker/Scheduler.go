package worker

import (
	"fmt"
	"github.com/gorhill/cronexpr"
	"github.com/shenzan/hackathon/protocol"
	"time"
)

// Scheduler is responsible for scheduling and executing jobs
type Scheduler struct {
	jobEventChan      chan *protocol.JobEvent              // etcd job event queue
	jobPlanTable      map[string]*protocol.JobSchedulePlan // job scheduling plan table
	jobExecutingTable map[string]*protocol.JobExecuteInfo  // job executing table
	jobResultChan     chan *protocol.JobExecuteResult      // job result queue
}

var (
	G_scheduler *Scheduler
)

// handleJobEvent handles different types of job events (SAVE, DELETE, KILL)
func (scheduler *Scheduler) handleJobEvent(jobEvent *protocol.JobEvent) {
	var (
		jobSchedulePlan *protocol.JobSchedulePlan
		jobExecuteInfo  *protocol.JobExecuteInfo
		jobExecuting    bool
		jobExisted      bool
		err             error
	)
	switch jobEvent.EventType {
	case protocol.JOB_EVENT_SAVE: // SAVE job event
		if jobSchedulePlan, err = buildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case protocol.JOB_EVENT_DELETE: // DELETE job event
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case protocol.JOB_EVENT_KILL: // KILL job event
		// Cancel the command execution and check if the job is currently running
		if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc() // Trigger command to kill the shell sub-process, and the job will exit
		}
	}
}


func (scheduler *Scheduler) handleJobResult(result *protocol.JobExecuteResult) {
	var (
		jobLog *protocol.JobLog
	)
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	if result.Err != protocol.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &protocol.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Output:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      result.EndTime.UnixNano() / 1000 / 1000,
			LocalIP:      result.LocalIP,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		G_MongoBatchDAO.Append(jobLog)
	}

}


// buildJobSchedulePlan builds the job scheduling plan using cron expression
func buildJobSchedulePlan(job *protocol.Job) (jobSchedulePlan *protocol.JobSchedulePlan, err error) {
	expr, err := cronexpr.Parse(job.CronExpr)
	if err != nil {
		return nil, err
	}

	jobSchedulePlan = &protocol.JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return jobSchedulePlan, nil
}

// TryStartJob tries to start a job if it's not currently running
func (scheduler *Scheduler) TryStartJob(jobPlan *protocol.JobSchedulePlan) {
	var (
		jobExecuteInfo *protocol.JobExecuteInfo
		jobExecuting   bool
	)

	// Job execution may take a long time, and scheduling it 60 times within 1 minute is unnecessary as it can only be executed once, to prevent concurrent execution.

	// If the job is currently running, skip this schedule
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		// fmt.Println("Not exited yet, skipping execution:", jobPlan.Job.Name)
		return
	}

	// Build execution status information
	jobExecuteInfo = protocol.BuildJobExecuteInfo(jobPlan)

	// Save the execution status
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// Execute the job
	fmt.Println("Executing job:", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)
}

// TrySchedule recalculates the job scheduling status
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan  *protocol.JobSchedulePlan
		now      time.Time
		nearTime *time.Time
	)

	// If the job table is empty, sleep for a while
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	// Current time
	now = time.Now()

	// Loop through all jobs
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) // Update the next execution time
		}

		// Calculate the nearest upcoming job time
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}
	// Next scheduling interval (nearest upcoming job time - current time)
	scheduleAfter = (*nearTime).Sub(now)
	return
}

// scheduleLoop is the main scheduling loop that handles job events and results
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent      *protocol.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *protocol.JobExecuteResult
	)

	// Initialize once
	scheduleAfter = scheduler.TrySchedule()

	// Initialize the scheduling timer
	scheduleTimer = time.NewTimer(scheduleAfter)

	// Main loop for scheduling
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: // Listen for job events
			// Update the job list maintained in memory based on the event type
			scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: // The nearest job is about to execute
		case jobResult = <-scheduler.jobResultChan: // Listen for job execution results
			scheduler.handleJobResult(jobResult)
		}
		// Schedule the job once
		scheduleAfter = scheduler.TrySchedule()
		// Reset the scheduling interval
		scheduleTimer.Reset(scheduleAfter)
	}Read task execution results
}

// PushJobEvent pushes a job event to the scheduler
func (scheduler *Scheduler) PushJobEvent(jobEvent *protocol.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// InitScheduler initializes the job scheduler
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *protocol.JobEvent, 1000),
		jobPlanTable:      make(map[string]*protocol.JobSchedulePlan),
		jobExecutingTable: make(map[string]*protocol.JobExecuteInfo),
		jobResultChan:     make(chan *protocol.JobExecuteResult, 1000),
	}
	// Start the scheduling loop
	go G_scheduler.scheduleLoop()
	return
}

// PushJobResult sends the job execution result back to the scheduler
func (scheduler *Scheduler) PushJobResult(jobResult *protocol.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}
