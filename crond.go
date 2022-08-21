package crond

import (
	"fmt"
	"github.com/dylisdong/crond/cron"
	"github.com/dylisdong/crond/driver"
	"log"
	"time"
)

const defaultDuration = time.Second * 1

type Crond struct {
	jobs           map[string]*JobWrapper
	serviceName    string
	updateDuration time.Duration
	nodePool       *NodePool
	cron           *cron.Cron
	opts           []cron.Option
	isRun          bool
}

func NewCrond(serviceName string, driver driver.Driver, opts ...Option) *Crond {
	crond := &Crond{
		serviceName:    serviceName,
		updateDuration: defaultDuration,
		jobs:           make(map[string]*JobWrapper),
		opts:           make([]cron.Option, 0),
	}

	for _, opt := range opts {
		opt(crond)
	}

	crond.cron = cron.New(crond.opts...)

	crond.nodePool = newNodePool(serviceName, driver, crond, crond.updateDuration)

	return crond
}

// AddJob add a job
func (c *Crond) AddJob(jobName, jobType, spec string, job cron.Job) error {
	return c.addJob(jobName, jobType, spec, nil, job)
}

// AddFunc add a cron func
func (c *Crond) AddFunc(jobName, jobType, spec string, cmd func()) error {
	return c.addJob(jobName, jobType, spec, cmd, nil)
}

func (c *Crond) addJob(jobName, jobType, spec string, cmd func(), job cron.Job) error {
	if _, ok := c.jobs[jobName]; ok {
		return fmt.Errorf("job[%s] already exists", jobName)
	}

	j := &JobWrapper{Job: job, Func: cmd, Crond: c, Name: jobName, Type: jobType}

	id, err := c.cron.AddJob(spec, j)
	if err != nil {
		return err
	}

	j.Id = id

	c.jobs[jobName] = j

	return nil
}

func (c *Crond) thisNodeRun(jobName string) bool {
	runNode := c.nodePool.PickNodeByJobName(jobName)

	log.Printf("info: job[%s] will running in node[%s]", jobName, runNode)

	if runNode == "" {
		log.Printf("error: node pool is empty")
		return false
	}

	return c.nodePool.nodeId == runNode
}

//Start Crond
func (c *Crond) Start() {
	c.isRun = true

	err := c.nodePool.StartWatch()
	if err != nil {
		c.isRun = false
		log.Printf("error: crond start watch failed: %+v", err)
		return
	}

	log.Printf("info: crond started, nodeId is %s", c.nodePool.nodeId)

	c.cron.Start()
}

// Run Crond
func (c *Crond) Run() {
	c.isRun = true

	err := c.nodePool.StartWatch()
	if err != nil {
		c.isRun = false
		log.Printf("error: crond start watch failed: %+v", err)
		return
	}

	log.Printf("info: crond running, nodeId is %s", c.nodePool.nodeId)

	c.cron.Run()
}

// Stop Crond
func (c *Crond) Stop() {
	c.isRun = false
	c.cron.Stop()
}

// Remove Job
func (c *Crond) Remove(jobName string) {
	if job, ok := c.jobs[jobName]; ok {
		delete(c.jobs, jobName)
		c.cron.Remove(job.Id)
	}
}
