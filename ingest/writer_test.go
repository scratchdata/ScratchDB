package ingest_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"scratchdb/client"
	"scratchdb/config"
	"scratchdb/ingest"
)

type sqsMock struct {
	sqsiface.SQSAPI

	t        *testing.T
	err      error
	expected *sqs.SendMessageOutput

	sendMessageCallCount int
	sendMessageInput     []*sqs.SendMessageInput
}

func (m *sqsMock) SendMessage(in *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	m.sendMessageCallCount += 1
	m.sendMessageInput = append(m.sendMessageInput, in)
	return m.expected, m.err
}

type s3Mock struct {
	s3iface.S3API

	t        *testing.T
	err      error
	expected *s3.PutObjectOutput

	putObjectCallInput [][2]string
}

func (m *s3Mock) PutObject(in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	file := in.Body.(*os.File)
	bb, err := os.ReadFile(file.Name())
	require.NoError(m.t, err)
	v := [2]string{file.Name(), string(bb)}
	m.putObjectCallInput = append(m.putObjectCallInput, v)
	return m.expected, m.err
}

func TestFileWriter(t *testing.T) {
	corpus := []string{
		"lorem ipsum",
		"dolor sit amet consectetur adipiscing elit",
		"sed do eiusmod tempor incididunt ut labore et dolore magna aliqua",
		" ", "",
	}
	testCases := map[string]struct {
		maxSizeBytes  int64
		maxAgeSeconds int

		writeCount int

		pushMessageErr error
		putObjectErr   error
		expectPanic    bool

		expectedPushMessageCount int
		expectedPutObjectCount   int
	}{
		"Basic": {
			maxAgeSeconds: 6,
			maxSizeBytes:  1000,

			expectedPushMessageCount: 1,
			expectedPutObjectCount:   1,
		},
		// TODO: include more test cases
		//"S3 Upload Error": {
		//	putObjectErr: errors.New("test putObject failed"),
		//	expectPanic:  true,
		//},
		//"SQS Push Error": {
		//	pushMessageErr: errors.New("test pushMessage failed"),
		//},
	}

	for name, tc := range testCases {
		name, tc := name, tc

		t.Run(name, func(t *testing.T) {
			dataDir, uploadDir := t.TempDir(), t.TempDir()
			cfg := config.Config{
				Storage: config.Storage{S3Bucket: "testBucket"},
				AWS:     config.AWS{SQS: "testQueue"},
				Ingest:  config.IngestConfig{MaxSizeBytes: tc.maxSizeBytes, MaxAgeSeconds: tc.maxAgeSeconds},
			}
			tags := map[string]string{"testTag": "tagged"}
			cl := client.Client{
				S3:  &s3Mock{t: t, err: tc.putObjectErr},
				SQS: &sqsMock{t: t, err: tc.pushMessageErr},
			}

			var logBuf bytes.Buffer
			sandbox(t, &logBuf, func() {
				writer := ingest.NewFileWriter(dataDir, &cfg, uploadDir, tags)
				writer.Client = &cl

				if tc.writeCount == 0 {
					tc.writeCount = len(corpus) - 1
				}

				for i := 0; i <= tc.writeCount; i++ {
					x, n := i, len(corpus)
					if x >= n {
						x = x % n
					}
					err := writer.Write(corpus[x])
					require.NoError(t, err)
					time.Sleep(1 * time.Second)
				}
				err := writer.Close()
				require.NoError(t, err)

			})

			fmt.Println("DEBUG::" + logBuf.String() + "END")

			s3, sqs := cl.S3.(*s3Mock), cl.SQS.(*sqsMock)
			t.Run("sqs pushMessage", func(t *testing.T) {
				assert.Equal(t, tc.expectedPushMessageCount, len(sqs.sendMessageInput))
			})

			t.Run("s3 putObject", func(t *testing.T) {
				assert.Equal(t, tc.expectedPutObjectCount, len(s3.putObjectCallInput))

				//TODO: verify contents of files properly
				fileContent := strings.Join(corpus, "\n") + "\n"
				assert.Equal(t, fileContent, s3.putObjectCallInput[0][1])
			})
		})
	}
}

func TestFileWriterMultipleRotations(t *testing.T) {
	// TODO: test for multiple file rotations
}

func sandbox(t *testing.T, lw io.Writer, f func()) {
	t.Helper()
	log.SetOutput(lw) // not concurrent-safe
	defer log.SetOutput(os.Stderr)

	f()
}
