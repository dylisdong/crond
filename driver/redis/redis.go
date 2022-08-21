package redis

import (
	"context"
	"github.com/dylisdong/crond/driver"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"log"
	"time"
)

type driverRedis struct {
	driver     *redis.Client
	ctx        context.Context
	expiration time.Duration
}

func NewDriver(r *redis.Client) driver.Driver {
	return &driverRedis{ctx: context.Background(), driver: r}
}

func (r *driverRedis) Ping() error { return r.driver.Ping(r.ctx).Err() }

func (r *driverRedis) SetExpiration(expiration time.Duration) { r.expiration = expiration }

func (r *driverRedis) Keepalive(nodeId string) { go r.keepalive(nodeId) }

func (r *driverRedis) GetServiceNodeList(serviceName string) ([]string, error) {
	var list []string

	// crond:{serviceName}:*
	match := driver.PrefixKey + driver.JAR + serviceName + driver.JAR + driver.REG

	iter := r.driver.Scan(r.ctx, 0, match, 0).Iterator()
	for iter.Next(r.ctx) {
		list = append(list, iter.Val())

		err := iter.Err()
		if err != nil {
			return list, err
		}
	}

	return list, nil
}

func (r *driverRedis) RegisterServiceNode(serviceName string) (string, error) {
	// crond:{serviceName}:{uuid}
	nodeId := driver.PrefixKey + driver.JAR + serviceName + driver.JAR + uuid.New().String()

	return nodeId, r.register(nodeId)
}

func (r *driverRedis) register(nodeId string) error {
	ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)

	err := r.driver.SetEX(ctx, nodeId, nodeId, r.expiration).Err()
	cancel()

	if err != nil {
		log.Printf("error: node[%s] register failed: %+v", nodeId, err)
	}

	return err
}

func (r *driverRedis) renewal(nodeId string) bool {
	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)

	ok, err := r.driver.Expire(ctx, nodeId, r.expiration).Result()
	cancel()

	if err != nil {
		log.Printf("error: node[%s] renewal failed: %+v", nodeId, err)
	}

	return ok
}

func (r *driverRedis) keepalive(nodeId string) {
	ticker := time.NewTicker(r.expiration / 2)
	defer ticker.Stop()

	for range ticker.C {
		if !r.renewal(nodeId) {
			_ = r.register(nodeId)
		}
	}
}
