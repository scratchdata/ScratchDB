package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"scratchdata/cmd"
	"scratchdata/cmd/api"
	"scratchdata/config"
	"scratchdata/pkg/database"
	"scratchdata/pkg/destinations"
	"scratchdata/pkg/filestore"
	dummystore "scratchdata/pkg/filestore/dummy"
	memorystore "scratchdata/pkg/filestore/memory"
	"scratchdata/pkg/filestore/s3"
	"scratchdata/pkg/queue"
	dummyqueue "scratchdata/pkg/queue/dummy"
	memoryqueue "scratchdata/pkg/queue/memory"
	"scratchdata/pkg/queue/sqs"
	"scratchdata/pkg/transport"
	"scratchdata/pkg/transport/memory"
	"scratchdata/pkg/transport/queuestorage"

	"github.com/BurntSushi/toml"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func setupLogs(logConfig config.Logs) {
	// Equivalent of Lshortfile
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		file = short
		return file + ":" + strconv.Itoa(line)
	}

	// Set log level
	zerolog.SetGlobalLevel(logConfig.ToLevel())

	// Set log output format
	if logConfig.Pretty {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Caller().Logger()
	} else {
		log.Logger = log.With().Caller().Logger()
	}
}

func getConfig(filePath string) config.Config {
	var conf config.Config
	if _, err := toml.DecodeFile(filePath, &conf); err != nil {
		log.Fatal().Err(err).Msg("Unable to load config file")
	}
	return conf
}

func main() {
	configFile := os.Args[1]
	config := getConfig(configFile)

	setupLogs(config.Logs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var db database.Database
	db = database.GetDB(config.Database)

	err := db.Open()
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to connect to database")
	}
	defer db.Close()

	var queueBackend queue.QueueBackend
	switch config.Queue {
	case "memory":
		queueBackend = memoryqueue.NewQueue()
	case "sqs":
		queueBackend = sqs.NewQueue(config.SQS)
	default:
		queueBackend = &dummyqueue.DummyQueue{}
	}

	var storageBackend filestore.StorageBackend
	switch config.Storage {
	case "memory":
		storageBackend = memorystore.NewStorage()
	case "s3":
		storageBackend = s3.NewStorage(config.S3)
	default:
		storageBackend = &dummystore.DummyStorage{}
	}

	var dataTransport transport.DataTransport

	switch config.Transport.Type {
	case "memory":
		dataTransport = memory.NewMemoryTransport(db)
	case "queuestorage":
		dataTransport = queuestorage.NewQueueStorageTransport(queuestorage.QueueStorageParam{
			Queue:   queueBackend,
			Storage: storageBackend,
			WriterOpt: queuestorage.WriterOptions{
				DataDir:     config.Transport.QueueStorage.ProducerDataDir,
				MaxFileSize: config.Transport.QueueStorage.MaxFileSizeBytes,
				MaxRows:     config.Transport.QueueStorage.MaxRows,
				MaxFileAge:  time.Duration(config.Transport.QueueStorage.MaxFileAgeSeconds) * time.Second,
			},
			DB:                     db,
			ConsumerDataDir:        config.Transport.QueueStorage.ConsumerDataDir,
			DequeueTimeout:         time.Duration(config.Transport.QueueStorage.DequeueTimeoutSeconds) * time.Second,
			FreeSpaceRequiredBytes: config.Transport.QueueStorage.FreeSpaceRequiredBytes,
			Workers:                config.Transport.Workers,
		})
	}

	// go dataTransport.StartProducer()

	go func() {
		if err := dataTransport.StartConsumer(); err != nil {
			// TODO: find a cleaner way to handle this.
			// if the consumer fails to start, we want to shutdown
			// but we can't call Fatal() because it kills the process
			// and potentially leaves the producer, etc. in an inconsistent state
			//
			// cancelling ctx would be good, but at the moment the API crashes when calling stop()
			log.Error().Err(err).Msg("Cannot start consumer")
		}
	}()

	commands := make([]cmd.Command, 0)
	if config.API.Enabled {
		commands = append(commands, api.NewAPIServer(config.API, db, dataTransport))
	}

	if len(commands) == 0 {
		log.Fatal().Msg("No services are enabled in config file")
	}

	for _, command := range commands {
		go func(command cmd.Command) {
			err := command.Start()
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to start service")
			}
		}(command)
	}

	select {
	case <-ctx.Done():
		for _, command := range commands {
			command.Stop()
		}

		// dataTransport.StopProducer()

		if err := dataTransport.StopConsumer(); err != nil {
			log.Info().Err(err).Msg("Cannot stop consumer")
		}

		if err := destinations.ClearCache(); err != nil {
			log.Info().Err(err).Msg("Cannot close database servers")
		}
	}
}
