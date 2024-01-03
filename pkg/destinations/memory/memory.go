package memory

import (
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"scratchdata/util"
	"sync"

	"github.com/rs/zerolog/log"
)

var (
	// selectStarFromPat matches the query `select * from (table)`
	selectStarFromPat = regexp.MustCompile(`(?i)^select\s+[*]\s+from\s+(\w+)$`)

	ErrClosed = errors.New("server is closed")
)

// MemoryDBServer implements an in-memory database server
//
// NOTE: All instances share the same backing store.
type MemoryDBServer struct {
	UseAllCaps bool `mapstructure:"all_caps"`

	mu     sync.RWMutex
	tables map[string][]json.RawMessage
	closed bool
}

// InsertBatchFromNDJson implements destinations.DatabaseServer.InsertBatchFromNDJson
func (m *MemoryDBServer) InsertBatchFromNDJson(table string, r io.ReadSeeker) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrClosed
	}

	dec := json.NewDecoder(r)
	for {
		var data json.RawMessage
		err := dec.Decode(&data)
		switch {
		case err == nil:
			m.tables[table] = append(m.tables[table], data)
			log.Debug().
				Str("table", table).
				Int("rows", len(m.tables[table])).
				Bytes("data", data).
				Msg("MemoryDBServer: Inserterting")
		case errors.Is(err, io.EOF):
			return nil
		default:
			log.Error().
				Str("table", table).
				Err(err).
				Msg("MemoryDBServer: Inserterting")
			return err
		}
	}
}

// QueryJSON implements destinations.DatabaseServer.QueryJSON
//
// If the query is `select * from $table` all rows inserted into $table will be returned
// Otherwise, if UseAllCaps is true, `[{"HELLO":"WORLD"}]` is returned
// Otherwise, `[{"hello":"world"}]` is returned
func (m *MemoryDBServer) QueryJSON(query string, w io.Writer) error {
	query = util.TrimQuery(query)

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return ErrClosed
	}

	rows := []json.RawMessage{}
	table := ""
	selectFrom := selectStarFromPat.FindStringSubmatch(query)
	switch {
	case len(selectFrom) == 2:
		table = selectFrom[1]
		rows = append(rows, m.tables[table]...)
	case m.UseAllCaps:
		rows = append(rows, json.RawMessage(`{"HELLO":"WORLD"}`))
	default:
		rows = append(rows, json.RawMessage(`{"hello":"world"}`))
	}

	data, err := json.Marshal(rows)
	if err != nil {
		return err
	}

	var tableNames []string
	for k := range m.tables {
		tableNames = append(tableNames, k)
	}

	n, err := w.Write(data)
	log.Debug().
		Str("query", query).
		Strs("tables", tableNames).
		Str("table", table).
		Int("rows", len(rows)).
		Int("bytes_written", n).
		Err(err).
		Msg("MemoryDBServer: Querying")
	return err
}

// Close implements destinations.DatabaseServer.Close
//
// On subsequent calls to close, ErrClosed is returned
func (m *MemoryDBServer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrClosed
	}
	m.closed = true
	return nil
}

// OpenServer returns a new initialized MemoryDBServer
func OpenServer(settings map[string]any) *MemoryDBServer {
	db := util.ConfigToStruct[MemoryDBServer](settings)
	db.tables = map[string][]json.RawMessage{}
	return db
}
