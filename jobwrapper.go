package crond

import "github.com/dylisdong/crond/cron"

const (
	JobLocaled     = "localed"
	JobDistributed = "distributed"
)

// JobWrapper is a job wrapper
type JobWrapper struct {
	Id   cron.EntryID
	Job  cron.Job
	Func func()

	Crond *Crond
	Name  string
	Type  string
}

func (job *JobWrapper) Run() {
	if job.Type == JobLocaled {
		job.run()
	}

	if job.Type == JobDistributed &&
		job.Crond.thisNodeRun(job.Name) {
		job.run()
	}
}

func (job *JobWrapper) run() {
	if job.Func != nil {
		job.Func()
	}

	if job.Job != nil {
		job.Job.Run()
	}
}
