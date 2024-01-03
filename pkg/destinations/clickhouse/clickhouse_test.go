package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
)

func TestQueue(t *testing.T) {
	database := "testdb"
	username := "testuser"
	password := ""

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Create pool: %s", err)
	}
	if err := pool.Client.Ping(); err != nil {
		t.Fatalf("Ping Docker: %s", err)
	}
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "clickhouse/clickhouse-server",
		Env: []string{
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
			"CLICKHOUSE_DB=" + database,
			"CLICKHOUSE_USER=" + username,
			"CLICKHOUSE_PASSWORD" + password,
		},
	})
	if err != nil {
		t.Fatalf("Run container: %s", err)
	}
	t.Cleanup(func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("Purge resource: %s", err)
		}
	})

	addr := resource.GetHostPort("9000/tcp")
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("Cannot split addr: %s: %s", addr, err)
	}
	tcpPort, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("Cannot parse port: %s: %s", port, err)
	}
	port = resource.GetPort("8123/tcp")
	httpPort, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("Cannot parse port: %s: %s", port, err)
	}

	dbSettings := map[string]any{
		"protocol":  "http",
		"host":      host,
		"http_port": httpPort,
		"tcp_port":  tcpPort,
		"username":  username,
		"password":  password,
		"database":  database,
	}
	var db *ClickhouseServer
	err = pool.Retry(func() error {
		db, err = OpenServer(dbSettings)
		return err
	})
	if err != nil {
		t.Fatalf("Cannot open server: %s", err)
	}

	// the implementation is currently incomplete so we manually create the table
	// columns will be added, so just add a dummy column
	err = db.conn.Exec(context.Background(), `
		create table if not exists `+database+`.tbl (_ Int) engine=Memory
	`)
	if err != nil {
		t.Fatalf("Cannot create table: %s", err)
	}

	if err := db.InsertBatchFromNDJson("tbl", strings.NewReader(`{"msg":"hello world"}`)); err != nil {
		t.Fatalf("Cannot insert JSON: %s", err)
	}
	buf := &bytes.Buffer{}
	if err := db.QueryJSON(`select * from tbl`, buf); err != nil {
		t.Fatalf("Cannot query JSON: %s", err)
	}
	type Msg struct{ Msg string }
	vals := []Msg{}
	if err := json.Unmarshal(buf.Bytes(), &vals); err != nil {
		t.Fatalf("Cannot decode JSON: %s", err)
	}
	exp := Msg{Msg: "hello world"}
	if len(vals) != 1 || vals[0] != exp {
		t.Fatalf(`Expected [%+v]; Got %+v`, exp, vals)
	}
}
