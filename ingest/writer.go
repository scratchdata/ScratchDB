package ingest

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"scratchdb/client"
	"scratchdb/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/oklog/ulid/v2"
)

type FileWriter struct {
	Client *client.Client

	// Where to save data
	DataDirectory string

	// Where in S3 to upload file
	UploadDirectory string

	// Extra metadata associated with each file
	Tags map[string]string

	// Number of workers for handling concurrent requests
	S3UploadWorkers int

	// To compress data before writing to a file
	// "none", "gzip", or "brotli"
	CompressionMethod string

	AWSConfig config.AWS

	Config *config.Config

	// Current file being written to
	fd *os.File

	// Push closed files to a longer term storage
	pusherDone chan bool

	// Ensure only 1 rotation is happening at a time
	rotating sync.Mutex
	// Ensure only 1 file write (or rotate) is happening at a time
	canWrite sync.Mutex

	// Used to rotate every x interval
	ticker *time.Ticker

	tickerDone chan bool

	wg sync.WaitGroup
}

func NewFileWriter(
	DataDirectory string,

	MaxAgeSeconds int,
	MaxSizeBytes int64,
	AWSConfig config.AWS,
	S3UploadWorkers int,
	CompressionMethod string,
	config *config.Config,

	UploadDirectory string,
	Tags map[string]string,
) *FileWriter {
	fw := &FileWriter{
		Client:            client.NewClient(config),
		DataDirectory:     DataDirectory,
		AWSConfig:         AWSConfig,
		S3UploadWorkers:   S3UploadWorkers,
		CompressionMethod: CompressionMethod,
		Config:            config,
		ticker:            time.NewTicker(time.Duration(config.Ingest.MaxAgeSeconds) * time.Second),
		tickerDone:        make(chan bool),
		pusherDone:        make(chan bool),
		UploadDirectory:   UploadDirectory,
		Tags:              Tags,
	}

	closedDir := filepath.Join(fw.DataDirectory, "closed")
	err := os.MkdirAll(closedDir, os.ModePerm)
	if err != nil {
		log.Println(err)
	}

	openDir := filepath.Join(fw.DataDirectory, "open")
	err = os.MkdirAll(openDir, os.ModePerm)
	if err != nil {
		log.Println(err)
	}

	// Kickstart the writer by creating a new file
	fw.Rotate(true)

	// Kickstart automatic file rotation on timer
	fw.wg.Add(1)
	go fw.rotateOnTimer()

	fw.wg.Add(1)
	go fw.pushFiles()

	return fw
}

func (f *FileWriter) rotateOnTimer() {
	defer f.wg.Done()

	for {
		select {
		case <-f.tickerDone:
			log.Println("Stopping ticker for", f.DataDirectory)
			return
		case <-f.ticker.C:
			// log.Println("Trying periodic rotate...")

			f.canWrite.Lock()
			fileinfo, err := f.fd.Stat()
			if err != nil {
				log.Println("Unable to auto rotate", err)
			}
			if fileinfo.Size() > 0 {
				f.Rotate(true)
			}
			f.canWrite.Unlock()
		}
	}
}

func (f *FileWriter) uploadS3File(filename string) error {
	path := filepath.Join(f.DataDirectory, "closed", filename)
	// log.Println("Uploading", path, "to s3")

	file, err := os.Open(path)
	if err != nil {
		log.Printf("os.Open - filename: %s, err: %v", path, err)
		return err
	}
	defer file.Close()

	s3Key := filepath.Join(f.UploadDirectory, filename)
	_, err = f.Client.S3.PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(f.Config.Storage.S3Bucket),
		Key:                aws.String(s3Key),
		Body:               file,
		ContentDisposition: aws.String("attachment"),
	})
	if err != nil {
		return err
	}

	sqsMessage := make(map[string]string)
	for k, v := range f.Tags {
		log.Println("Adding kv to sqs message", k, v)
		sqsMessage[k] = v
	}
	sqsMessage["bucket"] = f.Config.Storage.S3Bucket
	sqsMessage["key"] = s3Key
	log.Println("Final SQS message", sqsMessage)

	sqsPayload, err := json.Marshal(sqsMessage)
	if err != nil {
		return err
	}
	log.Println("SQS JSON Payload", string(sqsPayload))

	_, err = f.Client.SQS.SendMessage(
		&sqs.SendMessageInput{
			MessageBody: aws.String(string(sqsPayload)),
			QueueUrl:    &f.Config.AWS.SQS,
		})

	return err
}

// TODO: Ideally want to have a pool of workers who can upload
// Implemented:
// 1. Defined a set of goroutines and dispatched a goroutine for every file upload
// 2. Pushed the fileName to fileChannel from where the name is passed as input
func (f *FileWriter) pushFiles() {
	defer f.wg.Done()

	fileChan := make(chan string)

	var workerWG sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < f.S3UploadWorkers; i++ {
		workerWG.Add(1)
		go f.uploadWorker(fileChan, &workerWG)
	}

	keepReading := true
	for keepReading {
		select {
		case <-f.pusherDone:
			log.Println("Finishing uploading remaining files, then will stop")
			keepReading = false
		default:
		}

		// log.Println("Checking for files to upload")

		uploadPath := filepath.Join(f.DataDirectory, "closed")
		entries, err := os.ReadDir(uploadPath)
		if err != nil {
			log.Println(err)
			continue
		}

		for _, e := range entries {
			filename := filepath.Join(uploadPath, e.Name())
			fileinfo, err := e.Info()

			if err != nil {
				log.Println("Unable to get info for file", filename, err)
			}

			if fileinfo.Size() > 0 {

				fileChan <- filename
			}
		}

		time.Sleep(1 * time.Second)
	}

	close(fileChan)

	workerWG.Wait()
}

func (f *FileWriter) uploadWorker(fileChan <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	for filename := range fileChan {
		if err := f.uploadS3File(filepath.Base(filename)); err != nil {
			log.Println(err)
			log.Fatal(err)
		}

		// Remove the file after successful upload
		if err := os.Remove(filename); err != nil {
			log.Println("Unable to remove file", filename, err)
		}
	}
}

func (f *FileWriter) Rotate(createNew bool) error {
	// BLOCKS ALL WRITES while we rotate.
	// Could we be more clever here by opening the new file
	// in a new goroutine to continue write while we close the previous

	// Make sure only one rotation is happening at a time, as we do them asynchronously

	rotating := f.rotating.TryLock()
	if !rotating {
		log.Println("Someone else is currently rotating, skipping this rotation")
		return nil
	}
	defer f.rotating.Unlock()

	// log.Println("Rotating!")
	var err error

	// Check to see if we have an open fd
	if f.fd != nil {
		fileinfo, err := f.fd.Stat()
		if err != nil {
			log.Println(err)
			return err
		}

		oldName := f.fd.Name()
		filename := fileinfo.Name()

		err = f.fd.Close()
		if err != nil {
			log.Println(err)
			return err
		}

		newDir := filepath.Join(f.DataDirectory, "closed")
		err = os.MkdirAll(newDir, os.ModePerm)
		if err != nil {
			log.Println(err)
			return err
		}

		err = os.Rename(oldName, filepath.Join(newDir, filename))
		if err != nil {
			log.Println(err)
			return err
		}
	}

	if createNew {
		newFileId := ulid.Make().String()
		dir := filepath.Join(f.DataDirectory, "open")
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			log.Println(err)
			return err
		}

		path := filepath.Join(dir, newFileId+".ndjson")

		f.fd, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func (f *FileWriter) Write(data string) error {
	var err error

	f.canWrite.Lock()
	defer f.canWrite.Unlock()

	// check to see if we will hit our file size limit
	fileinfo, err := f.fd.Stat()
	if err != nil {
		log.Println(err)
		return err
	}
	if fileinfo.Size()+int64(len(data)) > f.Config.Ingest.MaxSizeBytes {
		err = f.Rotate(true)
	}
	if err != nil {
		log.Println(err)
		return err
	}

	var compressedData []byte
	switch f.CompressionMethod {
	case "gzip":
		var buff bytes.Buffer
		gw := gzip.NewWriter(&buff)
		_, err := gw.Write([]byte(data + "\n"))
		if err != nil {
			log.Println("Failed to compress data using gzip" + err.Error())
			return err
		}
		err = gw.Close()
		if err != nil {
			log.Println(err)
			return err
		}
		compressedData = buff.Bytes()
	case "brotli":
		// Logic for  brotli compression
	default:
		// No compression
		compressedData = []byte(data + "\n")
	}

	// write data
	// _, err = f.fd.WriteString(data + "\n")
	// if err != nil {
	// 	log.Println(err)
	// }

	// write data
	_, err = f.fd.Write(compressedData)
	if err != nil {
		log.Println(err)
		return err
	}

	return err
}

func (f *FileWriter) Close() error {
	f.ticker.Stop()
	f.tickerDone <- true

	// Close open file
	f.canWrite.Lock()
	err := f.Rotate(false)
	f.canWrite.Unlock()

	// Check on this error
	if err != nil {
		log.Println(err)
	}

	log.Println("Finishing uploading files")
	f.pusherDone <- true

	f.wg.Wait()

	return nil
}

func decompressData(compressedData []byte, compressionType string) (string, error) {
	var decompressedData string
	switch compressionType {
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(compressedData))
		if err != nil {
			log.Println("Error while creating a gzip reader: " + err.Error())
			return "", err
		}
		defer reader.Close()
		uncompressedData, err := io.ReadAll(reader)
		if err != nil {
			log.Println(err)
			return "", err
		}
		decompressedData = string(uncompressedData)

	case "brotli":
		// Brotli decompression logic

	default:
		decompressedData = string(compressedData)
	}
	return decompressedData, nil

}
