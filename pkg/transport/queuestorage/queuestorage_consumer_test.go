package queuestorage

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"scratchdata/models"
	"scratchdata/pkg/destinations"
	memFS "scratchdata/pkg/filestore/memory"
	memQ "scratchdata/pkg/queue/memory"

	"github.com/oklog/ulid/v2"
)

type testDB struct {
	Conn models.DatabaseConnection
}

func (d testDB) Open() error { return nil }

func (d testDB) Close() error { return nil }

func (d testDB) Hash(input string) string { return input }

func (d testDB) GetAPIKeyDetails(hashedKey string) models.APIKey { return models.APIKey{} }

func (d testDB) GetAccount(id string) models.Account { return models.Account{} }

func (d testDB) GetDatabaseConnections(accountID string) []models.DatabaseConnection {
	return []models.DatabaseConnection{d.Conn}
}

func (d testDB) GetDatabaseConnection(connectionID string) models.DatabaseConnection {
	return d.Conn
}

func (d testDB) HealthCheck() error { return nil }

func TestQueueStorageTransportConsumer(t *testing.T) {
	db := testDB{
		Conn: models.DatabaseConnection{
			ID:   ulid.Make().String(),
			Type: "memory",
		},
	}
	dbSrv, err := destinations.GetDestination(db.Conn)
	if err != nil {
		t.Fatal(err)
	}

	queue := memQ.NewQueue()
	store := memFS.NewStorage()
	param := QueueStorageParam{
		Queue:                  queue,
		Storage:                store,
		DB:                     db,
		ConsumerDataDir:        t.TempDir(),
		DequeueTimeout:         10 * time.Millisecond,
		FreeSpaceRequiredBytes: 100 << 20,
		Workers:                4,
	}

	qs := NewQueueStorageTransport(param)
	path := "/msg"
	uploadData := []byte(`{"data":"hello world"}`)
	table := "tbl"
	queueMsg, err := json.Marshal(models.FileUploadMessage{
		Path:  path,
		Key:   db.Conn.ID,
		Table: table,
	})
	expectRes := []byte(`[{"data":"hello world"}]`)
	if err != nil {
		t.Fatalf("Cannot marshal queueMsg: %s", err)
	}

	if err := qs.StartConsumer(); err != nil {
		t.Fatalf("Cannot start consumer: %s", err)
	}

	if err := store.Upload(path, bytes.NewReader(uploadData)); err != nil {
		t.Errorf("Cannot upload data: %s", err)
	}
	if err := queue.Enqueue(queueMsg); err != nil {
		t.Errorf("Cannot enqueue message: %s", err)
	}

	queryRes := bytes.NewBuffer(nil)
	for i := 0; i < 20 && queryRes.Len() == 0; i++ {
		// wait for data to be processed. it's all in-memory so shouldn't take long
		time.Sleep(50 * time.Millisecond)

		err := dbSrv.QueryJSON(`select * from tbl`, queryRes)
		if err != nil {
			t.Errorf("Cannot query db: %s", err)
			break
		}
	}

	if !bytes.Equal(queryRes.Bytes(), expectRes) {
		t.Errorf("Expected query result %#q; Got %#q", expectRes, queryRes.Bytes())
	}

	if err := qs.StopConsumer(); err != nil {
		t.Errorf("Cannot stop consumer: %s", err)
	}
}
