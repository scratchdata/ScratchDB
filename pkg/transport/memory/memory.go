package memory

import (
	"bytes"
	"scratchdata/pkg/database"
	"scratchdata/pkg/destinations"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type MemoryTransport struct {
	Workers int
	wg      sync.WaitGroup
	done    chan bool
	mutex   sync.Mutex
	ticker  *time.Ticker

	// map[dbID][tableName]tableData
	buffers map[string]map[string]*bytes.Buffer

	data chan Message
	db   database.Database
}

type Message struct {
	DBConnectionID string
	Data           []byte
}

func NewMemoryTransport(db database.Database) *MemoryTransport {
	rc := &MemoryTransport{
		buffers: make(map[string]map[string]*bytes.Buffer),
		ticker:  time.NewTicker(5 * time.Second),
		data:    make(chan Message),
		db:      db,
	}

	return rc
}

func (s *MemoryTransport) StartProducer() error {
	log.Info().Msg("Starting data producer")
	return nil
}

func (s *MemoryTransport) StopProducer() error {
	log.Info().Msg("Stopping data producer")
	return nil
}

func (s *MemoryTransport) Write(databaseConnectionId string, table string, data []byte) error {
	log.Trace().Bytes("data", data).Str("table", table).Str("db_conn", databaseConnectionId).Msg("writing")
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, ok := s.buffers[databaseConnectionId]
	if !ok {
		s.buffers[databaseConnectionId] = map[string]*bytes.Buffer{} //newBuf
	}

	tableData, _ := s.buffers[databaseConnectionId]

	buf, ok := tableData[table]
	if !ok {
		newBuf := &bytes.Buffer{}
		buf = newBuf
		tableData[table] = buf
	}

	buf.Write(data)
	buf.WriteByte('\n')

	return nil
}

func (s *MemoryTransport) StartConsumer() error {
	log.Info().Msg("Starting DB importer")

	go func() {
		for {
			select {
			case <-s.done:
				return
			case <-s.ticker.C:
				s.mutex.Lock()

				for dbID, tables := range s.buffers {
					for tableName, buf := range tables {
						if buf.Len() > 0 {
							connInfo := s.db.GetDatabaseConnection(dbID)
							conn, err := destinations.GetDestination(connInfo)
							if err != nil {
								log.Error().Err(err).Bytes("data", buf.Bytes()).Str("db", dbID).Str("table", tableName).Msg("Unable to get Destination")
								buf.Reset()
								continue
							}
							r := bytes.NewReader(buf.Bytes())
							err = conn.InsertBatchFromNDJson(tableName, r)
							if err != nil {
								log.Error().Err(err).Bytes("data", buf.Bytes()).Str("db", dbID).Str("table", tableName).Msg("Unable to save data to db")
							}
							buf.Reset()
						}
					}
				}
				s.mutex.Unlock()
			}
		}
	}()

	return nil
}

func (s *MemoryTransport) StopConsumer() error {
	log.Info().Msg("Shutting down data importer")
	return nil
}
