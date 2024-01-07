package destinations

import (
	"io"
	"scratchdata/models"
	"scratchdata/models/postgrest"
	"scratchdata/pkg/destinations/clickhouse"
	"scratchdata/pkg/destinations/duckdb"
	"scratchdata/pkg/destinations/memory"
	"scratchdata/util"
)

func GetDestination(dbConfig models.DatabaseConnection) DatabaseServer {
	configType := dbConfig.Type
	connectionSettings := dbConfig.ConnectionSettings

	switch configType {
	case "duckdb":
		return util.ConfigToStruct[duckdb.DuckDBServer](connectionSettings)
	case "clickhouse":
		return util.ConfigToStruct[clickhouse.ClickhouseServer](connectionSettings)
	case "memory":
		return util.ConfigToStruct[memory.MemoryDBServer](connectionSettings)
	default:
		return nil
	}
}

type DatabaseServer interface {
	InsertBatchFromNDJson(table string, input io.ReadSeeker) error
	QueryJSON(query string, writer io.Writer) error
	QueryPostgrest(query postgrest.Postgrest, writer io.Writer) error
}
