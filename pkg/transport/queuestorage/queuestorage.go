package queuestorage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"scratchdata/models"
	"scratchdata/pkg/database"
	"scratchdata/pkg/destinations"
	"scratchdata/pkg/filestore"
	"scratchdata/pkg/queue"
	"scratchdata/util"

	"github.com/rs/zerolog/log"
)

type WriterOptions struct {
	DataDir     string
	MaxFileSize int64
	MaxRows     int64
	MaxFileAge  time.Duration
}

type QueueStorageParam struct {
	Queue   queue.QueueBackend
	Storage filestore.StorageBackend

	WriterOpt WriterOptions // TODO: Refactor use of this

	DB                     database.Database
	ConsumerDataDir        string
	DequeueTimeout         time.Duration
	FreeSpaceRequiredBytes uint64
	Workers                int
}

type QueueStorage struct {
	queue   queue.QueueBackend
	storage filestore.StorageBackend

	DB                     database.Database
	ConsumerDataDir        string
	Workers                int
	DequeueTimeout         time.Duration
	FreeSpaceRequiredBytes uint64

	fws         map[string]*FileWriter
	fwsMu       sync.Mutex
	closedFiles chan FileWriterInfo

	wg   sync.WaitGroup
	done chan struct{}

	opt WriterOptions
}

func NewQueueStorageTransport(param QueueStorageParam) *QueueStorage {
	rc := &QueueStorage{
		queue:   param.Queue,
		storage: param.Storage,
		opt:     param.WriterOpt,

		fws:         make(map[string]*FileWriter),
		closedFiles: make(chan FileWriterInfo),

		done:                   make(chan struct{}),
		DB:                     param.DB,
		ConsumerDataDir:        param.ConsumerDataDir,
		DequeueTimeout:         param.DequeueTimeout,
		FreeSpaceRequiredBytes: param.FreeSpaceRequiredBytes,
		Workers:                param.Workers,
	}

	return rc
}

func (s *QueueStorage) StartProducer() error {
	log.Info().Msg("Starting data producer")
	return nil
}

func (s *QueueStorage) StopProducer() error {
	log.Info().Msg("Stopping data producer")

	var err error
	s.fwsMu.Lock()
	defer s.fwsMu.Unlock()
	for k, v := range s.fws {
		if closeErr := v.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("unable to close file")
			err = errors.Join(err, closeErr)
		}
		delete(s.fws, k)
	}

	return err
}

func (s *QueueStorage) Write(databaseConnectionId string, table string, data []byte) (err error) {
	s.fwsMu.Lock()
	defer s.fwsMu.Unlock()
	fw, ok := s.fws[databaseConnectionId]
	if !ok {
		var err error
		fw, err = NewFileWriter(FileWriterParam{
			Key:         databaseConnectionId,
			Dir:         s.opt.DataDir,
			Table:       table,
			MaxFileSize: s.opt.MaxFileSize,
			MaxRows:     s.opt.MaxRows,
			MaxFileAge:  s.opt.MaxFileAge,

			Queue:   s.queue,
			Storage: s.storage,
		})
		if err != nil {
			return err
		}
		s.fws[databaseConnectionId] = fw
	}

	if _, err = fw.Write(data); err != nil {
		return err
	}

	return nil
}

func (s *QueueStorage) StartConsumer() error {
	if s.ConsumerDataDir == "" {
		return fmt.Errorf("QueueStorage.StartConsumer: ConsumerDataDir is empty")
	}
	if s.Workers <= 0 {
		return fmt.Errorf("QueueStorage.StartConsumer: Workers should be >= 1")
	}
	if s.DequeueTimeout <= 0 {
		return fmt.Errorf("QueueStorage.StartConsumer: DequeueTimeout should be >= 1")
	}

	// _try_ to ensure the directory exists.
	// it can fail due to permissions, etc. so defer to the create() error
	os.MkdirAll(s.ConsumerDataDir, 0700)

	s.wg.Add(s.Workers)
	for i := 0; i < s.Workers; i++ {
		log.Info().Int("pid", i).Msg("Starting Consumer")
		go s.consumeMessages(i)
	}

	return nil
}

func (s *QueueStorage) StopConsumer() error {
	log.Info().Msg("Shutting down data consumer")
	close(s.done)
	s.wg.Wait()
	return nil
}

func (s *QueueStorage) insertMessage(msg models.FileUploadMessage) (retErr error) {
	dbID := msg.Key
	tableName := msg.Table
	defer func() {
		log.Debug().
			Str("dbID", dbID).
			Str("table", tableName).
			Any("message", msg).
			Err(retErr).
			Msg("QueueStorage: insertMessage")
	}()

	conn := s.DB.GetDatabaseConnection(dbID)
	if conn.ID == "" {
		return fmt.Errorf("QueueStorage.insertMessage: Cannot get database connection for '%s'", dbID)
	}

	dest, err := destinations.GetDestination(conn)
	if err != nil {
		return fmt.Errorf("QueueStorage.insertMessage: Cannot get destination for '%s/%s': %w", dbID, conn.ID, err)
	}

	fn := filepath.Join(s.ConsumerDataDir, filepath.Base(msg.Path))
	file, err := os.Create(fn)
	if err != nil {
		return fmt.Errorf("QueueStorage.insertMessage: Cannot create '%s': %w", fn, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Error().
				Err(err).
				Str("file", fn).
				Str("db", dbID).
				Str("table", tableName).
				Msg("Closing data file failed")
		}

		// keep the file if insertion failed
		if retErr == nil {
			os.Remove(file.Name())
		}
	}()

	if err := s.storage.Download(msg.Path, file); err != nil {
		return fmt.Errorf("QueueStorage.insertMessage: Cannot download '%s': %w", msg.Path, err)
	}

	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("QueueStorage.insertMessage: Cannot reset file offset: %w", err)
	}
	if err = dest.InsertBatchFromNDJson(tableName, file); err != nil {
		log.Error().
			Err(err).
			Str("file", fn).
			Str("db", dbID).
			Str("table", tableName).
			Msg("Unable to save data to db")
	}

	return nil
}

func (s *QueueStorage) consumeMessages(pid int) {
	defer s.wg.Done()

	for {
		select {
		case <-s.done:
			return
		default:
		}

		// Ensure we haven't filled up disk
		// TODO: ensure we have enough disk space for: max file upload size, temporary file for insert statement, add'l overhead
		// Could farm this out to AWS batch with a machine sized for the data.
		//
		// Since these workers are running concurrently, the maximum data size that we can expect to
		// download is also added, to avoid downloading anything if we might run out of space in the process
		requiredFreeBytes := s.FreeSpaceRequiredBytes + (uint64(s.Workers) * uint64(s.opt.MaxFileSize))
		if util.FreeDiskSpace(s.ConsumerDataDir) <= requiredFreeBytes {
			log.Error().Int("pid", pid).Msg("Disk is full, not consuming any messages")
			select {
			case <-time.After(1 * time.Minute):
				continue
			case <-s.done:
				return
			}
		}

		// TODO: implement timeout/cancellation in the queue backends
		// if StopConcumar is called while the queue is busy, we block here until it's done
		data, err := s.queue.Dequeue()
		if err != nil {
			if !errors.Is(err, queue.ErrEmpyQueue) {
				log.Error().Int("pid", pid).Err(err).Msg("Could not dequeue message")
			}
			select {
			// TODO: implement polling in the queue backends
			case <-time.After(s.DequeueTimeout):
				continue
			case <-s.done:
				return
			}
		}

		msg := models.FileUploadMessage{}
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Error().Int("pid", pid).Err(err).Bytes("message", data).Msg("Could not parse message")
			continue
		}

		if err := s.insertMessage(msg); err != nil {
			log.Error().
				Int("pid", pid).
				Str("path", msg.Path).
				Str("key", msg.Key).
				Err(err).
				Msg("Cannot insert message")
		}
	}
}
