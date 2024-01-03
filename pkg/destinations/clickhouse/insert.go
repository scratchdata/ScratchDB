package clickhouse

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
)

func (s *ClickhouseServer) inferColumnTypes(file io.ReadSeeker) (map[string]string, error) {
	rc := map[string]string{}

	typeCounts := map[string]map[string]int{}

	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return rc, err
	}

	scanner := bufio.NewScanner(file)
	maxCapacity := 100_000_000
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		parsed := gjson.ParseBytes(scanner.Bytes())

		parsed.ForEach(func(key, value gjson.Result) bool {
			k := key.String()
			_, ok := typeCounts[k]
			if !ok {
				typeCounts[k] = map[string]int{}
			}
			switch value.Type {
			case gjson.String:
				typeCounts[k]["string"] += 1
			case gjson.Null:
				typeCounts[k]["null"] += 1
			case gjson.False:
				typeCounts[k]["bool"] += 1
			case gjson.True:
				typeCounts[k]["bool"] += 1
			case gjson.Number:
				_, intErr := strconv.Atoi(value.Raw)
				if intErr != nil {
					typeCounts[k]["float"] += 1
				} else {
					typeCounts[k]["int"] += 1
				}
			default:
				typeCounts[k]["undefined"] += 1
			}
			return true
		})
	}

	log.Trace().Interface("column_type_counts", typeCounts).Send()

	if err := scanner.Err(); err != nil {
		return rc, err
	}

	for colName, colTypeCounts := range typeCounts {
		if colTypeCounts["string"] > 0 {
			rc[colName] = "string"
			continue
		} else if colTypeCounts["undefined"] > 0 {
			rc[colName] = "string"
			continue
		} else if colTypeCounts["float"] > 0 {
			rc[colName] = "float"
			continue
		} else if colTypeCounts["int"] > 0 {
			rc[colName] = "int"
			continue
		} else if colTypeCounts["bool"] > 0 {
			rc[colName] = "bool"
		} else {
			rc[colName] = "string"
		}

	}

	log.Trace().Interface("column_types", rc).Send()

	return rc, nil
}

func (s *ClickhouseServer) createColumnsWithTypes(table string, columns map[string]string) error {
	sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" `, s.Database, table)
	columnSql := []string{}
	for colName, jsonType := range columns {
		var colType string
		switch jsonType {
		case "int":
			colType = "Int64"
		case "bool":
			colType = "Boolean"
		case "float":
			colType = "Float64"
		case "string":
			colType = "String"
		default:
			colType = "String"
		}
		columnSql = append(columnSql, fmt.Sprintf(`ADD COLUMN IF NOT EXISTS "%s" %s`, colName, colType))
	}

	sql += strings.Join(columnSql, ", ")

	log.Trace().Msg(sql)

	return s.conn.Exec(context.TODO(), sql)
}

func (s *ClickhouseServer) getClickhouseTypes(table string) (map[string]string, error) {
	rc := map[string]string{}

	sql := fmt.Sprintf("DESCRIBE TABLE \"%s\" FORMAT JSON", table)
	resp, err := s.httpQuery(sql)
	defer resp.Close()

	if err != nil {
		return rc, err
	}

	data, err := io.ReadAll(resp)
	if err != nil {
		return rc, err
	}

	parsed := gjson.ParseBytes(data)
	for _, field := range parsed.Get("data").Array() {
		rc[field.Get("name").String()] = field.Get("type").String()
	}

	log.Trace().Interface("clickhouse_column_types", rc).Str("table", table).Send()
	return rc, nil
}

func (s *ClickhouseServer) jsonToGoType(clickhouseType string, data gjson.Result) any {
	switch clickhouseType {
	case "String", "FixedString":
		return data.String()
	case "Decimal":
		return decimal.NewFromFloat(data.Float())
	case "Bool":
		return data.Bool()
	case "UInt8":
		return uint8(data.Uint())
	case "UInt16":
		return uint16(data.Uint())
	case "UInt32":
		return uint32(data.Uint())
	case "UInt64":
		return data.Uint()
	case "UInt128", "UInt256":
		n := new(big.Int)
		n.SetString(data.String(), 10)
		return n
	case "Int8":
		return int8(data.Int())
	case "Int16":
		return int16(data.Int())
	case "Int32":
		return int32(data.Int())
	case "Int64":
		return data.Int()
	case "Int128", "Int256":
		n := new(big.Int)
		n.SetString(data.String(), 10)
		return n
	case "Float32":
		return float32(data.Float())
	case "Float64":
		return data.Float()
	case "UUID":
		return data.String()
	case "Date", "Date32":
		return data.String()
	case "DateTime", "DateTime64":
		if data.Type == gjson.Number {
			return data.Int()
		} else {
			return data.String()
		}
	case "Enum8":
		return int8(data.Int())
	case "Enum16":
		return int16(data.Int())
	}

	return data.String()
}

func (s *ClickhouseServer) insertData(file io.ReadSeeker, table string, columns map[string]string) error {
	// Get list of columns so we use the same order
	colNames := make([]string, len(columns))
	i := 0
	for k := range columns {
		colNames[i] = k
		i++
	}

	// Get types for clickhouse columns
	clickhouseColumnTypes, err := s.getClickhouseTypes(table)
	if err != nil {
		return err
	}

	// Create INSERT statement
	insertSql := fmt.Sprintf(`INSERT INTO "%s"."%s" (`, s.Database, table)
	for i, colName := range colNames {
		insertSql += fmt.Sprintf("`%s`", colName)
		if i < len(columns)-1 {
			insertSql += ","
		}
	}
	insertSql += ")"

	// log.Trace().Str("key", localFile).Msg(insertSql)

	// Open .ndjson file for reading
	_, err = file.Seek(0, io.SeekStart)

	// file, err := os.Open(localFile)
	if err != nil {
		return err
	}
	// defer file.Close()

	// Begin batch
	batch, err := s.conn.PrepareBatch(context.Background(), insertSql)
	if err != nil {
		log.Err(err).Msg("unable to initiate batch query")
		return err
	}

	scanner := bufio.NewScanner(file)
	maxCapacity := 100_000_000
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	// Iterate over each JSON object
	row := 0
	for scanner.Scan() {
		data := scanner.Bytes()
		vals := make([]any, len(colNames))

		for i, colName := range colNames {
			colType := clickhouseColumnTypes[colName]
			vals[i] = s.jsonToGoType(colType, gjson.GetBytes(data, colName))
		}

		err = batch.Append(vals...)
		if err != nil {
			log.Error().Err(err).Int("row", row).Msg("Unable to add item to batch")
		}
		row++
	}

	if err := scanner.Err(); err != nil {
		log.Err(err).Msg("scanner error")
		batch.Abort()
		return err
	}

	return batch.Send()
}

func (s *ClickhouseServer) InsertBatchFromNDJson(table string, input io.ReadSeeker) error {

	columns, err := s.inferColumnTypes(input)
	if err != nil {
		log.Err(err).Msg("failed to retrieve columns from input JSON")
		return err
	}

	err = s.createColumnsWithTypes(table, columns)
	if err != nil {
		log.Err(err).Msg("failed to create columns")
		return err
	}

	err = s.insertData(input, table, columns)
	if err != nil {
		log.Err(err).Msg("Failed to insert data")
		return err
	}

	return nil
}
