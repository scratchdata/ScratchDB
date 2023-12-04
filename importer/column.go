package importer

import (
	"errors"
	"fmt"
	"strings"
)

type Kind uint

const (
	Nullable Kind = iota
	Bool
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
	Float32
	Float64

	DateTime
	DateTime64

	// TODO: Complete other types
)

func (k Kind) String() string {
	m := map[Kind]string{
		Nullable: "Nullable(Nothing)",
		Bool:     "Boolean",
		String:   "String",

		Int8:   "Int8",
		Int16:  "Int16",
		Int32:  "Int32",
		Int64:  "Int64",
		Int128: "Int128",
		Int256: "Int256",

		UInt8:   "UInt8",
		UInt16:  "UInt16",
		UInt32:  "UInt32",
		UInt64:  "UInt64",
		UInt128: "UInt128",
		UInt256: "UInt256",

		Float:   "FLOAT",
		Double:  "DOUBLE",
		Float32: "Float32",
		Float64: "Float64",

		DateTime:   "DateTime()",
		DateTime64: "DateTime64(9)",
	}
	return m[k]
}

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

var _ fmt.Stringer = Kind(0)
