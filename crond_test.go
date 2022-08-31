package crond

import (
	"fmt"
	"github.com/dylisdong/crond/driver/mysql"
	"github.com/go-redis/redis/v8"
	"go.etcd.io/etcd/client/v3"
	my "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"os/signal"
	"syscall"
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
	d := mysql.NewDriver(clientMysql(), "")

	crond := NewCrond("test-service", d, WithLazyPick(true))

	_ = crond.AddFunc("job", JobDistributed, "*/1 * * * *", func() {
		fmt.Println("执行job: ", time.Now().Format("15:04:05"))
	})

	crond.Start()

	signalCh := make(chan os.Signal)

	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case sig := <-signalCh:
		fmt.Println("received stop signal: ", sig)
		crond.Stop()
		time.Sleep(10 * time.Second)
	}
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

func clientMysql() *gorm.DB {
	db, err := gorm.Open(my.Open("root:123456@tcp(127.0.0.1:3306)/tmp?parseTime=True"))
	if err != nil {
		fmt.Println(err)
	}

	return db
}
