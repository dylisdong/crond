package mysql

import (
	"context"
	"fmt"
	"github.com/dylisdong/crond/driver"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"log"
	"strings"
	"time"
)

const (
	defaultTTL = 15 // 15 second ttl
	ctxTimeout = 5 * time.Second

	defaultTable = "crond"
)

var tableName = defaultTable

type driverMysql struct {
	driver *gorm.DB
	ctx    context.Context
	ttl    int64

	cancelCh chan struct{}
}

func NewDriver(r *gorm.DB, table string) driver.Driver {
	if table != "" {
		tableName = table
	}
	return &driverMysql{ctx: context.Background(), driver: r, cancelCh: make(chan struct{})}
}

func (m *driverMysql) Ping() error { return m.driver.AutoMigrate(&Table{}) }

func (m *driverMysql) SetKeepaliveInterval(interval time.Duration) {
	ttl := int64(interval.Seconds())

	if ttl < defaultTTL {
		ttl = defaultTTL
	}

	m.ttl = ttl
}

func (m *driverMysql) Keepalive(nodeId string) { go m.keepalive(nodeId) }

func (m *driverMysql) GetServiceNodeList(serviceName string) (nodeIds []string, err error) {
	ctx, cancel := context.WithTimeout(m.ctx, ctxTimeout)
	defer cancel()

	var list []*Table

	err = m.driver.Session(&gorm.Session{NewDB: true}).
		WithContext(ctx).
		Where("service = ?", serviceName).
		Where("state = ?", running).
		Where("expired_at >= ?", time.Now().Unix()-m.ttl).
		Find(&list).Error

	if err != nil {
		return
	}

	for _, v := range list {
		nodeIds = append(nodeIds, v.Service+driver.JAR+v.Node)
	}
	return
}

func (m *driverMysql) RegisterServiceNode(serviceName string) (nodeId string, err error) {
	nodeId = serviceName + driver.JAR + uuid.NewString()

	return nodeId, m.register(nodeId)
}

func (m *driverMysql) UnRegisterServiceNode() { m.cancelCh <- struct{}{} }

func (m *driverMysql) register(nodeId string) error {
	list := strings.Split(nodeId, driver.JAR)
	if len(list) != 2 {
		return fmt.Errorf("nodeId[%s] invalid", nodeId)
	}

	ctx, cancel := context.WithTimeout(m.ctx, ctxTimeout)
	defer cancel()

	return m.driver.Session(&gorm.Session{NewDB: true}).
		WithContext(ctx).
		Save(&Table{
			Service:   list[0],
			Node:      list[1],
			State:     running,
			ExpiredAt: time.Now().Unix() + m.ttl,
		}).Error
}

func (m *driverMysql) unregister(nodeId string) error {
	list := strings.Split(nodeId, driver.JAR)
	if len(list) != 2 {
		return fmt.Errorf("nodeId[%s] invalid", nodeId)
	}

	ctx, cel := context.WithTimeout(m.ctx, ctxTimeout)
	defer cel()

	return m.driver.Session(&gorm.Session{NewDB: true}).
		WithContext(ctx).Model(&Table{}).Where("node = ?", list[1]).
		Update("state", cancel).
		Error
}

func (m *driverMysql) keepalive(nodeId string) {
	list := strings.Split(nodeId, driver.JAR)

	node := list[1]

	ticker := time.NewTicker(time.Duration(m.ttl) * time.Second / 2)
	defer ticker.Stop()

	for {
		select {
		case <-m.cancelCh:
			err := m.unregister(nodeId)
			if err != nil {
				log.Printf("error: node[%s] unregister failed: [%+v]", nodeId, err)
			}

			return

		case <-ticker.C:
			ctx, cancel := context.WithTimeout(m.ctx, ctxTimeout)

			err := m.driver.Session(&gorm.Session{NewDB: true}).
				WithContext(ctx).Model(&Table{}).Where("node = ?", node).
				Update("expired_at", time.Now().Unix()).Error

			cancel()

			if err != nil {
				log.Printf("error: node[%s] renewal failed: [%+v]", nodeId, err)

				if err := m.register(nodeId); err != nil {
					log.Printf("error: node[%s] register failed: [%+v]", nodeId, err)
				}
			}
		}
	}
}

type Table struct {
	Id        int64  `gorm:"column:id; primaryKey; autoIncrement"`
	Service   string `gorm:"column:service; index; type:varchar(64)"`
	Node      string `gorm:"column:node; uniqueIndex; type:varchar(36)"`
	State     int8   `gorm:"column:state; type:tinyint"` // 1: running 2: cancel
	ExpiredAt int64  `gorm:"column:expired_at"`
}

func (t *Table) TableName() string { return tableName }

const (
	running = 1
	cancel  = 2
)
