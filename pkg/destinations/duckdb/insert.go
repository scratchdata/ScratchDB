package duckdb

import (
	"fmt"
	"io"
	"scratchdata/util"

	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
)

func (s *DuckDBServer) createTable(table string) error {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (__row_id STRING)", table)
	_, err := s.db.Exec(sql)
	return err
}

func (s *DuckDBServer) createColumns(table string, jsonTypes map[string]string) error {
	for colName, jsonType := range jsonTypes {

		// TODO: Should we specify defaults, or just use null as default?
		sql := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table, colName, jsonToDuck[jsonType])
		_, err := s.db.Exec(sql)
		if err != nil {
			return err
		}

		// sql = fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL", table, colName)
	}

	return nil
}

func (s *DuckDBServer) describeTable(table string) ([]string, map[string]string, error) {
	duckColumns := []string{}
	duckdbColTypes := make(map[string]string)

	sql := fmt.Sprintf("DESCRIBE \"%s\"", table)
	rows, err := s.db.Query(sql)
	if err != nil {
		return duckColumns, duckdbColTypes, err
	}

	for rows.Next() {
		var colName, colType string
		var isNull, key, defaultValue, extra *string
		if err := rows.Scan(&colName, &colType, &isNull, &key, &defaultValue, &extra); err != nil {
			return duckColumns, duckdbColTypes, err
		}

		duckdbColTypes[colName] = colType
		duckColumns = append(duckColumns, colName)
	}

	if err := rows.Err(); err != nil {
		return duckColumns, duckdbColTypes, err
	}

	return duckColumns, duckdbColTypes, err
}

func (s *DuckDBServer) insertFromS3(table string, tempFile string) error {
	sql := fmt.Sprintf(`
		INSERT INTO "%s" 
		BY NAME
		SELECT * FROM
		read_ndjson_auto(
			's3://%s/%s?s3_region=%s&s3_access_key_id=%s&s3_secret_access_key=%s&s3_endpoint=%s&s3_use_ssl=true'
		 )
		`,
		table, s.Bucket, tempFile, s.Region, s.AccessKeyId, s.SecretAccessKey, s.Endpoint,
	)

	_, err := s.db.Exec(sql)
	return err
}

func (s *DuckDBServer) InsertBatchFromNDJson(table string, input io.ReadSeeker) error {

	// Infer JSON types for the input
	jsonTypes, err := util.GetJSONTypes(input)
	if err != nil {
		return err
	}

	err = s.createTable(table)
	if err != nil {
		return err
	}

	err = s.createColumns(table, jsonTypes)
	if err != nil {
		return err
	}

	// duckColumns, duckdbColTypes, err := s.describeTable(table, db)
	// if err != nil {
	// 	return err
	// }

	// TODO: just pass the name of the local file or S3 bucket so we don't have to
	// copy data around

	tempFile := s.S3Prefix + "/temp/" + ulid.Make().String() + ".ndjson"
	err = s.writeS3File(input, tempFile)
	if err != nil {
		return err
	}

	err = s.insertFromS3(table, tempFile)
	if err != nil {
		return err
	}

	err = s.deleteS3File(tempFile)
	if err != nil {
		log.Error().Err(err).Str("filename", tempFile).Msg("Unable to delete temp file from s3")
	}

	return nil
}
