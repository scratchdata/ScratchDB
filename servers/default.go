package servers

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/scratchdata/scratchdb/config"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type DefaultServerManager struct {
	serverConfigs   []config.ClickhouseConfig
	serverList      []ClickhouseServer
	apiKeyToServer  map[string][]ClickhouseServer
	dbNameToServer  map[string][]ClickhouseServer
	clusterToServer map[string][]ClickhouseServer
}

func NewDefaultServerManager(servers []config.ClickhouseConfig) ClickhouseManager {
	rc := DefaultServerManager{
		serverConfigs:   servers,
		serverList:      make([]ClickhouseServer, len(servers)),
		apiKeyToServer:  make(map[string][]ClickhouseServer),
		dbNameToServer:  make(map[string][]ClickhouseServer),
		clusterToServer: make(map[string][]ClickhouseServer),
	}

	for i, serverConfig := range rc.serverConfigs {
		server := &DefaultServer{server: &rc.serverConfigs[i]}
		rc.serverList[i] = server

		for _, apiKey := range serverConfig.HostedAPIKeys {
			rc.dbNameToServer[apiKey] = append(rc.dbNameToServer[apiKey], server)
		}

		for _, dbName := range serverConfig.HostedDBs {
			rc.dbNameToServer[dbName] = append(rc.dbNameToServer[dbName], server)
		}

		for _, cluster := range serverConfig.HostedClusters {
			rc.clusterToServer[cluster] = append(rc.clusterToServer[cluster], server)
		}
	}

	return &rc
}

func (m *DefaultServerManager) GetServersByAPIKey(apiKey string) []ClickhouseServer {
	return m.dbNameToServer[apiKey]
}

func (m *DefaultServerManager) GetServersByDBName(dbName string) []ClickhouseServer {
	return m.dbNameToServer[dbName]
}

func (m *DefaultServerManager) GetServersByDBCluster(dbCluster string) []ClickhouseServer {
	return m.clusterToServer[dbCluster]
}

func (m *DefaultServerManager) GetServers() []ClickhouseServer {
	return m.serverList
}

type DefaultServer struct {
	server *config.ClickhouseConfig
	conn   driver.Conn
	mutex  sync.Mutex
}

func (s *DefaultServer) GetHost() string {
	return s.server.Host
}

func (s *DefaultServer) GetPort() int {
	return s.server.TCPPort
}

func (s *DefaultServer) GetHttpPort() int {
	return s.server.HTTPPort
}

func (s *DefaultServer) GetHttpProtocol() string {
	return s.server.HTTPProtocol
}

func (s *DefaultServer) GetRootUser() string {
	return s.server.Username
}

func (s *DefaultServer) GetRootPassword() string {
	return s.server.Password
}

func (s *DefaultServer) GetStoragePolicy() string {
	return s.server.StoragePolicy
}

func (s *DefaultServer) UseTLS() bool {
	return s.server.TLS
}

func (s *DefaultServer) getMaxOpenConns() int {
	return s.server.MaxOpenConns
}

func (s *DefaultServer) getMaxIdleConns() int {
	return s.server.MaxIdleConns
}

func (s *DefaultServer) getConnMaxLifetimeSecs() int {
	return s.server.ConnMaxLifetimeSecs
}

func (s *DefaultServer) Connection() (driver.Conn, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// If the connection hasn't been initialized then create it
	if s.conn == nil {
		options := &clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", s.GetHost(), s.GetPort())},
			Auth: clickhouse.Auth{
				Username: s.GetRootUser(),
				Password: s.GetRootPassword(),
			},
			Debug:           false,
			MaxOpenConns:    s.getMaxOpenConns(),
			MaxIdleConns:    s.getMaxIdleConns(),
			ConnMaxLifetime: time.Second * time.Duration(s.getConnMaxLifetimeSecs()),
			DialTimeout:     120 * time.Second,
		}

		if s.UseTLS() {
			options.TLS = &tls.Config{
				InsecureSkipVerify: true,
			}
		}

		var ctx = context.Background()
		var conn, err = clickhouse.Open(options)

		if err != nil {
			return nil, err
		}

		if err := conn.Ping(ctx); err != nil {
			if exception, ok := err.(*clickhouse.Exception); ok {
				fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			}
			return nil, err
		}

		s.conn = conn
	}

	return s.conn, nil
}
