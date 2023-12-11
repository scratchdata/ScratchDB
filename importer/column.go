package importer

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"scratchdb/servers"
)

//go:generate stringer -type=Kind -linecomment

type Kind uint

const (
	Nullable Kind = iota // Nullable(Nothing)
	Bool
	Boolean
	String

	Int8
	Int16
	Int32
	Int64
	Int128
	Int256

	UInt8
	UInt16
	UInt32
	UInt64
	UInt128
	UInt256

	Float
	Double
	Float32 // Float32
	Float64 // Float64

	Date
	Date32
	DateTime   // DateTime()
	DateTime64 // DateTime64(9)

	LONGTEXT   // LONGTEXT
	MEDIUMTEXT // MEDIUMTEXT
	TINYTEXT   // TINYTEXT
	TEXT       // TEXT
	LONGBLOB   // LONGBLOB
	MEDIUMBLOB // MEDIUMBLOB
	TINYBLOB   // TINYBLOB
	BLOB       // BLOB
	VARCHAR    // VARCHAR
	CHAR       // CHAR

	TINYINT // TINYINT
	INT1    // INT1

	SMALLINT // SMALLINT
	INT2     // INT2

	INT     // INT
	INT4    // INT4
	INTEGER // INTEGER

	BIGINT // BIGINT

	// TODO: Complete other types
)

func (k Kind) Default() any {
	switch k {
	case Nullable:
		return "NULL"
	case Bool:
		return false
	case String:
		return "''"
	case DateTime, DateTime64:
		return 0 // equivalent to Unix timestamp 0
	case Int8, Int16, Int32, Int64, Int128, Int256:
		fallthrough
	case UInt8, UInt16, UInt32, UInt64, UInt128, UInt256:
		fallthrough
	case Float, Double, Float32, Float64:
		fallthrough
	default:
		return 0
	}
}

func (k Kind) Nullable() string {
	if k == Nullable {
		return k.String()
	}
	return fmt.Sprintf("Nullable(%s)", k)
}

func (k Kind) Aligns(x Kind) bool {
	aliasGroups := [][]Kind{
		{Bool, Boolean},
		{String, LONGTEXT, MEDIUMTEXT, TINYTEXT, TEXT,
			LONGBLOB, MEDIUMBLOB, TINYBLOB, BLOB, VARCHAR, CHAR},
		{Int8, TINYINT, INT1},
		{Int16, SMALLINT, INT2},
		{Int32, INT, INT4, INTEGER},
		{Int64, BIGINT},
		{Float, Float32},
		{Double, Float64},
	}

	for _, aliases := range aliasGroups {
		if slices.Contains(aliases, k) {
			return slices.Contains(aliases, x)
		}
	}

	return k == x
}

type Column struct {
	// Column is the name table column
	Name string

	// Type is the data type of the column (e.g., String, Int64, Decimal)
	Type Kind

	// Whether null values are allowed
	Nullable bool
}

func generateAlterColumnQuery(db, table string, columns []Column, enforceStringOnly bool) (string, error) {
	if len(columns) == 0 {
		return "", errors.New("alter query requires at least one column manipulation")
	}

	var addClauses []string
	for _, col := range columns {
		name := strings.ReplaceAll(col.Name, ".", "_")
		colType := col.Type
		if enforceStringOnly {
			colType = String
		}

		colTypeStr := colType.String()
		if col.Nullable {
			colTypeStr = fmt.Sprintf("Nullable(%s)", colType)
		}

		clause := fmt.Sprintf(
			`ADD COLUMN IF NOT EXISTS %s %s DEFAULT %v`,
			name, colTypeStr, col.Type.Default(),
		)
		addClauses = append(addClauses, clause)
	}

	addClausesStr := strings.Join(addClauses, ", ")
	sql := fmt.Sprintf(`
		ALTER TABLE %s.%s %s
	`, db, table, addClausesStr,
	)
	return sql, nil
}

func parseColumnType(colType string) (Kind, error) {
	if colType == "" {
		return Kind(0), errors.New("column type is empty")
	}

	pattern := regexp.MustCompile(`^(Nullable\()?(\w+)(\(.*\))?\)?$`)
	match := pattern.FindStringSubmatch(colType)
	colType = strings.ToLower(match[2])

	for i := 0; true; i++ {
		endMarker := fmt.Sprintf("Kind(%d)", i)

		k := Kind(i)
		kType := k.String()
		if kType == endMarker {
			break
		}

		if strings.ToLower(kType) == colType {
			return k, nil
		}
	}

	return Kind(0), fmt.Errorf("unknown column type %s", colType)
}

func inspectTableColumns(
	ctx context.Context,
	server servers.ClickhouseServer,
	db, table string) ([]Column, error) {
	conn, err := server.Connection()
	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf(`
		SELECT column_name, column_type, is_nullable
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE table_schema = '%s' AND table_name = '%s';
	`, db, table)
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		columns []Column
		allErrs error
	)
	for rows.Next() {
		var (
			name, colType string
			nullable      string
		)
		if err := rows.Scan(&name, &colType, &nullable); err != nil {
			allErrs = errors.Join(allErrs, err)
			continue
		}
		colKind, err := parseColumnType(colType)
		if err != nil {
			allErrs = errors.Join(allErrs, err)
			continue
		}

		col := Column{
			Name:     name,
			Type:     colKind,
			Nullable: nullable == "1",
		}
		columns = append(columns, col)
	}

	return columns, allErrs
}

type ColumnCompareResult struct {
	Aligned []Column
	Fresh   []Column
	Ignored []Column
}

func compareColumns(columns, colInfoSchema []Column) ColumnCompareResult {
	colInfoMap := map[string]*Column{}
	for _, col := range colInfoSchema {
		colInfoMap[col.Name] = &col
	}

	var rv ColumnCompareResult
	for _, col := range columns {
		info, ok := colInfoMap[col.Name]
		if !ok {
			// new columns are okay
			rv.Fresh = append(rv.Fresh, col)
			continue
		} else if col.Type.Aligns(info.Type) {
			rv.Aligned = append(rv.Aligned, col)
		} else {
			// ignore non-aligned existing columns
			rv.Ignored = append(rv.Ignored, col)
		}
	}

	return rv
}

var _ fmt.Stringer = Kind(0)
