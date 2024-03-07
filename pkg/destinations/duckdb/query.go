package duckdb

import (
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/util"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

func (s *DuckDBServer) QueryJSONPipe(query string, writer io.Writer) error {
	sanitized := util.TrimQuery(query)

	dir, err := os.MkdirTemp("", "scratchdata_duckdb")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	fifoPath := filepath.Join(dir, "p.pipe")
	log.Trace().Str("pipe", fifoPath).Msg("DuckDB pipe")
	err = syscall.Mkfifo(fifoPath, 0666)
	if err != nil {
		return err
	}
	defer os.Remove(fifoPath)

	done := make(chan error)

	sql := "COPY (" + sanitized + ") TO '" + fifoPath + "' (FORMAT JSON, ARRAY true)"
	log.Trace().Str(sql, sql).Send()

	// Execute query in one goroutine. This will block while
	// sending results to pipe, otherwise it will block while
	// sending an error
	go func() {
		log.Print(11)
		_, err := s.db.Exec(sql)
		log.Print(22)
		if err != nil {
			done <- err
		}
		log.Print(33)
	}()

	// Copy data from pipe to web output
	// This will block while waiting for pipe
	go func() {
		defer log.Print("DEAD")
		log.Print(1)
		pipe, err := os.Open(fifoPath)
		log.Print(2)
		if err != nil {
			done <- err
			return
		}
		defer pipe.Close()
		log.Print(3)

		_, err = io.Copy(writer, pipe)
		log.Print(4)
		log.Print(err)
		done <- err
		log.Print(5)
	}()

	// Who sends us the first completion? The DB executing or the copy?
	err = <-done
	if err != nil {
		return err
	}

	return nil
}

func (s *DuckDBServer) QueryJSONString(query string, writer io.Writer) error {
	sanitized := util.TrimQuery(query)

	rows, err := s.db.Query("DESCRIBE " + sanitized)
	if err != nil {
		return err
	}

	var columnName string
	var columnType *string
	var null *string
	var key *string
	var defaultVal *interface{}
	var extra *string
	columnNames := make([]string, 0)

	for rows.Next() {
		err := rows.Scan(&columnName, &columnType, &null, &key, &defaultVal, &extra)
		if err != nil {
			return err
		}
		columnNames = append(columnNames, columnName)
	}

	rows.Close()

	rows, err = s.db.Query("SELECT to_json(COLUMNS(*)) FROM (" + sanitized + ")")
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	writer.Write([]byte("["))

	// https://groups.google.com/g/golang-nuts/c/-9h9UwrsX7Q
	pointers := make([]interface{}, len(cols))
	container := make([]*string, len(cols))

	for i, _ := range pointers {
		pointers[i] = &container[i]
	}

	hasNext := rows.Next()
	for hasNext {
		err := rows.Scan(pointers...)
		if err != nil {
			return err
		}

		writer.Write([]byte("{"))
		for i, _ := range cols {
			writer.Write([]byte("\""))
			writer.Write([]byte(util.JsonEscape(columnNames[i])))
			writer.Write([]byte("\""))

			writer.Write([]byte(":"))

			if container[i] == nil {
				writer.Write([]byte("null"))
			} else {
				writer.Write([]byte(*container[i]))
			}

			if i < len(cols)-1 {
				writer.Write([]byte(","))
			}
		}

		writer.Write([]byte("}"))

		hasNext = rows.Next()

		if hasNext {
			writer.Write([]byte(","))
		}
	}

	writer.Write([]byte("]"))

	return nil
}
func (s *DuckDBServer) QueryJSON(query string, writer io.Writer) error {
	//return s.QueryJSONString(query, writer)
	return s.QueryJSONPipe(query, writer)
}
