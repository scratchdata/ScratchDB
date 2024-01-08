package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"scratchdata/config"
	"scratchdata/models"
	"scratchdata/pkg/database"
	memFS "scratchdata/pkg/filestore/memory"
	memQ "scratchdata/pkg/queue/memory"
	"scratchdata/pkg/transport/queuestorage"
)

const apiKey = "testAPIKey"

type testDB struct {
	database.Database
}

func (d testDB) Hash(input string) string { return input }

func (d testDB) GetAPIKeyDetails(hashedKey string) models.APIKey {
	return models.APIKey{
		ID:           hashedKey,
		HashedAPIKey: hashedKey,
	}
}

func (d testDB) GetDatabaseConnection(connectionID string) models.DatabaseConnection {
	return models.DatabaseConnection{ID: "test", Type: "memory"}
}

func TestInsertRequirements(t *testing.T) {
	testCases := []struct {
		name    string
		payload string
		header  http.Header
		query   url.Values

		writeCount int
		statusCode int
		respBody   string
	}{
		{
			name:       "Unauthorised request",
			payload:    `{"property":"A"}`,
			statusCode: 401,
			respBody:   "Unauthorized",
		},
		{
			name:    "Reject invalid JSON",
			payload: `"property`,
			header: http.Header{
				API_KEY_HEADER: []string{apiKey},
			},
			statusCode: 400,
			respBody:   "invalid JSON",
		},
		{
			name:    "Require table name",
			payload: `[{"property":"A"},{"property":"B"}]`,
			header: http.Header{
				API_KEY_HEADER: []string{apiKey},
			},
			statusCode: 400,
			respBody:   "missing required table field",
		},
		{
			name:    "Require data property for payload",
			payload: `{"table": "testTable"}`,
			header: http.Header{
				API_KEY_HEADER: []string{apiKey},
			},
			statusCode: 400,
			respBody:   "missing required data field",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {

			cfg := config.API{
				Enabled:                true,
				Port:                   0,
				DataDir:                "",
				MaxAgeSeconds:          0,
				MaxSizeBytes:           0,
				HealthCheckPath:        "",
				FreeSpaceRequiredBytes: 0,
			}
			db := testDB{}

			qsParam := queuestorage.QueueStorageParam{
				Queue:   memQ.NewQueue(),
				Storage: memFS.NewStorage(),
				WriterOpt: queuestorage.WriterOptions{
					DataDir:     t.TempDir(),
					MaxFileSize: 1,
					MaxRows:     1,
					MaxFileAge:  1 * time.Second,
				},
			}

			qs := queuestorage.NewQueueStorageTransport(qsParam)
			api := NewAPIServer(cfg, db, qs)

			payload := strings.NewReader(test.payload)
			req := httptest.NewRequest(http.MethodPost, "/data", payload)
			if test.header != nil {
				req.Header = test.header
			}
			resp, err := api.app.Test(req)
			require.NoError(t, err)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, test.statusCode, resp.StatusCode)
			assert.Equal(t, test.respBody, string(body))
		})
	}
}

func TestAPI_Insert(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	testCases := []struct {
		name    string
		payload string
		header  http.Header
		query   url.Values

		writeCount     int
		statusCode     int
		respBody       string
		contentPattern string
	}{
		{
			name:    "Read table and flatten from header",
			payload: `[{"property":"A"},{"property":"B"}]`,
			header: http.Header{
				API_KEY_HEADER:  []string{apiKey},
				TableNameHeader: []string{"testTable"},
				FlattenHeader:   []string{"explode"},
			},
			statusCode:     200,
			respBody:       "ok",
			writeCount:     2,
			contentPattern: fmt.Sprintf(`^{"property":"(A|B)","__row_id":"\w{26}","__batch_file":"%s(.+).ndjson"}$`, dataDir),
		},
		{
			name:    "Read table and flatten from url",
			payload: `[{"property":"A"},{"property":"B"}]`,
			header: http.Header{
				API_KEY_HEADER: []string{apiKey},
			},
			query: url.Values{
				TableNameQuery: []string{"testTable"},
				FlattenQuery:   []string{"explode"},
			},
			statusCode:     200,
			respBody:       "ok",
			writeCount:     2,
			contentPattern: fmt.Sprintf(`^{"property":"(A|B)","__row_id":"\w{26}","__batch_file":"%s(.+).ndjson"}$`, dataDir),
		},
		{
			name:    "Read table and flatten from body",
			payload: `{"table":"testTable","data":{"property":"A"}}`,
			header: http.Header{
				API_KEY_HEADER: []string{apiKey},
			},
			statusCode:     200,
			respBody:       "ok",
			writeCount:     1,
			contentPattern: fmt.Sprintf(`^{"property":"A","__row_id":"\w{26}","__batch_file":"%s(.+).ndjson"}$`, dataDir),
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			cfg := config.API{
				Enabled:                true,
				Port:                   0,
				DataDir:                "",
				MaxAgeSeconds:          0,
				MaxSizeBytes:           0,
				HealthCheckPath:        "",
				FreeSpaceRequiredBytes: 0,
			}
			db := testDB{}

			qsParam := queuestorage.QueueStorageParam{
				Queue:   memQ.NewQueue(),
				Storage: memFS.NewStorage(),
				WriterOpt: queuestorage.WriterOptions{
					DataDir:     dataDir,
					MaxFileSize: 2000,
					MaxRows:     100,
					MaxFileAge:  3 * time.Second,
				},
			}
			qs := queuestorage.NewQueueStorageTransport(qsParam)
			api := NewAPIServer(cfg, db, qs)

			payload := strings.NewReader(test.payload)
			target := "/data"
			if test.query != nil {
				target += "?" + test.query.Encode()
			}
			req := httptest.NewRequest(http.MethodPost, target, payload)
			if test.header != nil {
				req.Header = test.header
			}
			resp, err := api.app.Test(req)
			require.NoError(t, err)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, test.statusCode, resp.StatusCode)
			assert.Equal(t, test.respBody, string(body))

			time.Sleep(qsParam.WriterOpt.MaxFileAge + time.Second)

			bb, err := qsParam.Queue.Dequeue()
			require.NoError(t, err)

			var msg models.FileUploadMessage
			require.NoError(t, json.Unmarshal(bb, &msg))

			bb, err = os.ReadFile(msg.Path)
			require.NoError(t, err)

			bb = bytes.TrimSpace(bb)
			log.Debug().Str("contents", string(bb)).Msg("file conts")
			parts := bytes.Split(bb, []byte{'\n'})
			require.Equal(t, test.writeCount, len(parts))

			t.Run("content matches", func(t *testing.T) {
				for _, part := range parts {
					assert.Regexp(t, regexp.MustCompile(test.contentPattern), string(part))
				}
			})
		})
	}
}
