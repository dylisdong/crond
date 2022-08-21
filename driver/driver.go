package driver

import "time"

const (
	PrefixKey = "crond"
	JAR       = ":"
	REG       = "*"
)

//Driver is a driver interface
type Driver interface {
	// Ping is check driver is valid
	Ping() error
	Keepalive(nodeId string)
	SetExpiration(expiration time.Duration)
	GetServiceNodeList(serviceName string) (nodeIds []string, err error)
	RegisterServiceNode(serviceName string) (nodeId string, err error)
}
