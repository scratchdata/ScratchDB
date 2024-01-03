package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"scratchdata/util"
	"time"

	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
)

type DuckDBServer struct {
	Database string `mapstructure:"database"`
	Token    string `mapstructure:"token"`

	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Bucket          string `mapstructure:"bucket"`
	Region          string `mapstructure:"region"`
	S3Prefix        string `mapstructure:"s3_prefix"`
	Endpoint        string `mapstructure:"endpoint"`

	MaxOpenConns        int `mapstructure:"max_open_conns"`
	MaxIdleConns        int `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSecs int `mapstructure:"conn_max_lifetime_secs"`

	db *sql.DB
}

func (s *DuckDBServer) Close() error {
	return s.db.Close()
}

var jsonToDuck = map[string]string{
	"string": "STRING",
	"int":    "BIGINT",
	"float":  "DOUBLE",
	"bool":   "BOOLEAN",
}

func openDB(s *DuckDBServer) (*sql.DB, error) {
	connector, err := duckdb.NewConnector("md:"+s.Database+"?motherduck_token="+s.Token, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'json'",
			"LOAD 'json'",
			"INSTALL 'aws'",
			"LOAD 'aws'",
			"INSTALL 'httpfs'",
			"LOAD 'httpfs'",
		}

		for _, qry := range bootQueries {
			_, err := execer.ExecContext(context.TODO(), qry, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)
	db.SetConnMaxLifetime(time.Duration(s.ConnMaxLifetimeSecs) * time.Second)
	db.SetMaxIdleConns(s.MaxIdleConns)
	db.SetMaxOpenConns(s.MaxOpenConns)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, err
}

func OpenServer(settings map[string]any) (*DuckDBServer, error) {
	srv := util.ConfigToStruct[DuckDBServer](settings)
	db, err := openDB(srv)
	if err != nil {
		return nil, fmt.Errorf("OpenServer: %w", err)
	}
	srv.db = db
	return srv, nil
}
