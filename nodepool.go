package crond

import (
	"github.com/dylisdong/crond/driver"
	"github.com/dylisdong/crond/hash"
	"log"
	"sync"
	"time"
)

type NodePool struct {
	mu sync.Mutex

	serviceName    string
	nodeId         string
	updateDuration time.Duration

	driver driver.Driver
	crond  *Crond

	nodes *hash.ConsistentHash
}

func newNodePool(serviceName string, driver driver.Driver, crond *Crond, updateDuration time.Duration) *NodePool {
	err := driver.Ping()
	if err != nil {
		panic(err)
	}

	return &NodePool{
		serviceName:    serviceName,
		driver:         driver,
		crond:          crond,
		updateDuration: updateDuration,
	}
}

func (n *NodePool) StartWatch() error {
	n.driver.SetExpiration(n.updateDuration)

	nodeId, err := n.driver.RegisterServiceNode(n.serviceName)
	if err != nil {
		return err
	}

	n.driver.Keepalive(nodeId)

	n.nodeId = nodeId

	err = n.update()
	if err != nil {
		return err
	}

	go n.tickerUpdate()

	return nil
}

func (n *NodePool) update() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	nodeIds, err := n.driver.GetServiceNodeList(n.serviceName)
	if err != nil {
		return err
	}

	n.nodes = hash.NewConsistentHash()

	n.nodes.Add(nodeIds...)

	return nil
}

func (n *NodePool) tickerUpdate() {
	ticker := time.NewTicker(n.updateDuration)
	defer ticker.Stop()

	for range ticker.C {
		if n.crond.isRun {
			err := n.update()
			if err != nil {
				log.Printf("error: update node pool failed: %+v", err)
			}
		} else {
			return
		}
	}
}

func (n *NodePool) PickNodeByJobName(jobName string) string {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.nodes.IsEmpty() {
		return ""
	}
	return n.nodes.Get(jobName)
}
