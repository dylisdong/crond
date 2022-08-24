package crond

import (
	"fmt"
	"github.com/dylisdong/crond/driver/etcd"
	"github.com/go-redis/redis/v8"
	"go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func TestNewCrond(t *testing.T) {

	go node()

	time.Sleep(time.Second)

	go node()

	time.Sleep(time.Second * 2)

	go node()

	time.Sleep(time.Second * 60 * 10)
}

func node() {
	d := etcd.NewDriver(clientEtcd())

	crond := NewCrond("test-service", d, WithLazyPick(true))

	_ = crond.AddFunc("job", JobDistributed, "*/1 * * * *", func() {
		fmt.Println("执行job: ", time.Now().Format("15:04:05"))
	})

	crond.Start()
}

func clientRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Network:      "tcp",
		Addr:         "127.0.0.1:6379",
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 1 * time.Second,
	})
}

func clientEtcd() *clientv3.Client {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println(err)
	}
	return c
}
