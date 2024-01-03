package clickhouse

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"scratchdata/util"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog/log"
)

type ClickhouseServer struct {
	HTTPProtocol string `mapstructure:"protocol"`
	Host         string `mapstructure:"host"`
	HTTPPort     int    `mapstructure:"http_port"`
	TCPPort      int    `mapstructure:"tcp_port"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
	Database     string `mapstructure:"database"`

	StoragePolicy string `mapstructure:"storage_policy"`

	MaxOpenConns        int  `mapstructure:"max_open_conns"`
	MaxIdleConns        int  `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSecs int  `mapstructure:"conn_max_lifetime_secs"`
	TLS                 bool `mapstructure:"tls"`

	conn driver.Conn
}

func openConn(s *ClickhouseServer) (driver.Conn, error) {
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", s.Host, s.TCPPort)},
		Auth: clickhouse.Auth{
			Username: s.Username,
			Password: s.Password,
		},
		Debug:           false,
		MaxOpenConns:    s.MaxOpenConns,
		MaxIdleConns:    s.MaxIdleConns,
		ConnMaxLifetime: time.Second * time.Duration(s.ConnMaxLifetimeSecs),
		DialTimeout:     120 * time.Second,
	}

	if s.TLS {
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
			log.Err(err).
				Int("code", int(exception.Code)).
				Str("message", exception.Message).
				Str("stackTrace", exception.StackTrace).
				Send()
		}
		return nil, err
	}

	return conn, nil
}

func (s *ClickhouseServer) Close() error {
	return s.conn.Close()
}

func (s *ClickhouseServer) httpQuery(query string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s://%s:%d", s.HTTPProtocol, s.Host, s.HTTPPort)

	var jsonStr = []byte(query)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Clickhouse-User", s.Username)
	req.Header.Set("X-Clickhouse-Key", s.Password)
	req.Header.Set("X-Clickhouse-Database", s.Database)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("request failed")
		return nil, err
	}

	return resp.Body, nil
}

func OpenServer(settings map[string]any) (*ClickhouseServer, error) {
	srv := util.ConfigToStruct[ClickhouseServer](settings)
	conn, err := openConn(srv)
	if err != nil {
		return nil, fmt.Errorf("OpenServer: %w", err)
	}
	srv.conn = conn
	return srv, nil
}
