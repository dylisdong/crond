package etcd

import (
	"context"
	"github.com/dylisdong/crond/driver"
	"github.com/google/uuid"
	"go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

const (
	defaultTTL = 5 // 5 second ttl
	ctxTimeout = 5 * time.Second
)

type driverEtcd struct {
	driver *clientv3.Client
	ctx    context.Context
	ttl    int64

	aliveCh <-chan *clientv3.LeaseKeepAliveResponse
	leaseId clientv3.LeaseID
}

func NewDriver(driver *clientv3.Client) driver.Driver {
	return &driverEtcd{driver: driver, ctx: context.Background()}
}

func (e *driverEtcd) Ping() error { return nil }

func (e *driverEtcd) SetKeepaliveInterval(interval time.Duration) {
	ttl := int64(interval.Seconds())

	if ttl < defaultTTL {
		ttl = defaultTTL
	}

	e.ttl = ttl
}

func (e *driverEtcd) Keepalive(nodeId string) { go e.keepalive(nodeId) }

func (e *driverEtcd) GetServiceNodeList(serviceName string) (nodeIds []string, err error) {
	ctx, cancel := context.WithTimeout(e.ctx, ctxTimeout)
	defer cancel()

	var (
		list []string
		//  /crond/{serviceName}/
		prefix = driver.SPL + driver.PrefixKey + driver.SPL + serviceName + driver.SPL
	)

	gets, err := e.driver.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return list, err
	}

	for _, kv := range gets.Kvs {
		list = append(list, string(kv.Key))
	}

	return list, nil
}

func (e *driverEtcd) RegisterServiceNode(serviceName string) (nodeId string, err error) {
	//         /crond/{serviceName}/{uuid}
	nodeId = driver.SPL + driver.PrefixKey + driver.SPL + serviceName + driver.SPL + uuid.NewString()

	return nodeId, e.register(nodeId)
}

func (e *driverEtcd) register(nodeId string) error {
	ctx, cancel := context.WithTimeout(e.ctx, ctxTimeout)
	defer cancel()

	grant, err := e.driver.Grant(ctx, e.ttl)
	if err != nil {
		return err
	}

	e.leaseId = grant.ID

	_, err = e.driver.Put(ctx, nodeId, "ok", clientv3.WithLease(e.leaseId))
	if err != nil {
		return err
	}

	e.aliveCh, err = e.driver.KeepAlive(context.Background(), e.leaseId)
	if err != nil {
		return err
	}

	return nil
}

func (e *driverEtcd) keepalive(nodeId string) {
	ticker := time.NewTicker(time.Duration(e.ttl) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if e.aliveCh == nil {
				err := e.register(nodeId)
				if err != nil {
					log.Printf("error: node[%s] register failed: %+v", nodeId, err)
				}
			}

		case res := <-e.aliveCh:
			if res == nil {
				err := e.register(nodeId)
				if err != nil {
					log.Printf("error: node[%s] register failed: %+v", nodeId, err)
				}
			}
		}
	}
}
