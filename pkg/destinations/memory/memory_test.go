package memory

import (
	"bytes"
	"testing"
)

func TestMemoryDBServer(t *testing.T) {
	tblInput := []byte(`
		{"msg1": "hello"}
		{"msg2": "world"}
	`)
	tblOutput := []byte(`[{"msg1":"hello"},{"msg2":"world"}]`)
	defOutput := []byte(`[{"hello":"world"}]`)
	noOutput := []byte(`[]`)
	dest := OpenServer(nil)

	if err := dest.InsertBatchFromNDJson("tbl", bytes.NewReader(tblInput)); err != nil {
		t.Fatalf("Insert failed: %s", err)
	}

	buf := bytes.NewBuffer(nil)
	if err := dest.QueryJSON(`select * from tbl`, buf); err != nil {
		t.Fatalf("Query tbl: %s", err)
	}
	if res := buf.Bytes(); !bytes.Equal(res, tblOutput) {
		t.Fatalf("Expected %#q; Got %#q", tblOutput, res)
	}

	buf.Reset()
	// query matches `select * from table` but we didn't insert anything so it should return an empty array
	if err := dest.QueryJSON(`select * from missing_tbl`, buf); err != nil {
		t.Fatalf("Query missing_tbl: %s", err)
	}
	if res := buf.Bytes(); !bytes.Equal(res, noOutput) {
		t.Fatalf("Expected %#q; Got %#q", noOutput, res)
	}

	buf.Reset()
	// query doesn't match `select * from table` so it should return a default output
	if err := dest.QueryJSON(`select * from tbl where 1`, buf); err != nil {
		t.Fatalf("Query missing_tbl: %s", err)
	}
	if res := buf.Bytes(); !bytes.Equal(res, defOutput) {
		t.Fatalf("Expected %#q; Got %#q", defOutput, res)
	}
}
