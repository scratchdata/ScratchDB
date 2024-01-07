package memory

import (
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"scratchdata/models/postgrest"
	"scratchdata/util"
	"sync"

	"github.com/rs/zerolog/log"
)

var (
	// selectStarFromPat matches the query `select * from (table)`
	selectStarFromPat = regexp.MustCompile(`(?i)^select\s+[*]\s+from\s+(\w+)$`)

	// destinations.GetDestination returns a new object each time - so any data inserted, gets discarded
	// store the data as a singleton at least for now so we can query it later
	tables = struct {
		sync.Mutex
		m map[string][]json.RawMessage
	}{
		m: map[string][]json.RawMessage{},
	}
)

// MemoryDBServer implements an in-memory database server
//
// NOTE: All instances share the same backing store.
type MemoryDBServer struct {
	UseAllCaps bool `mapstructure:"all_caps"`
}

// InsertBatchFromNDJson implements destinations.DatabaseServer.InsertBatchFromNDJson
func (m MemoryDBServer) InsertBatchFromNDJson(table string, r io.ReadSeeker) error {
	tables.Lock()
	defer tables.Unlock()

	dec := json.NewDecoder(r)
	for {
		var data json.RawMessage
		err := dec.Decode(&data)
		switch {
		case err == nil:
			tables.m[table] = append(tables.m[table], data)
			log.Debug().
				Str("table", table).
				Int("rows", len(tables.m[table])).
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
// Otherwise, `[{"hello":"world"}]` or
func (m MemoryDBServer) QueryJSON(query string, w io.Writer) error {
	query = util.TrimQuery(query)

	tables.Lock()
	defer tables.Unlock()

	rows := []json.RawMessage{}
	table := ""
	selectFrom := selectStarFromPat.FindStringSubmatch(query)
	switch {
	case len(selectFrom) == 2:
		table = selectFrom[1]
		rows = append(rows, tables.m[table]...)
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
	for k := range tables.m {
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

func (m MemoryDBServer) QueryPostgrest(query postgrest.Postgrest, w io.Writer) error {
	return errors.New("Not implemented")
}
