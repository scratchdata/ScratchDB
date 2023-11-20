package servers

import "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

type ClickhouseManager interface {
	GetServers() []ClickhouseServer
	GetServersByAPIKey(apiKey string) []ClickhouseServer
	GetServersByDBName(dbName string) []ClickhouseServer
	GetServersByDBCluster(dbCluster string) []ClickhouseServer
}

type ClickhouseServer interface {
	GetHost() string
	GetPort() int
	UseTLS() bool
	GetHttpPort() int
	GetHttpProtocol() string

	GetRootUser() string
	GetRootPassword() string

	GetStoragePolicy() string

	Connection() (driver.Conn, error)
}
