package config

import (
	"github.com/rs/zerolog"
)

type Config struct {
	AccountManager map[string]interface{} `toml:"account_manager"`
	Database       map[string]interface{} `toml:"database"`

	Logs Logs `toml:"logs"`

	Queue   string `toml:"queue"`
	Storage string `toml:"storage"`

	S3  S3  `toml:"s3"`
	SQS SQS `toml:"sqs"`
	API API `toml:"api"`

	Transport Transport `toml:"transport"`
}

type Logs struct {
	Pretty bool `toml:"pretty"`
	// panic, fatal, error, warn, info, debug, trace
	Level string `toml:"level"`
}

func (loggingConfig Logs) ToLevel() zerolog.Level {
	switch loggingConfig.Level {
	case "panic":
		return zerolog.PanicLevel
	case "fatal":
		return zerolog.FatalLevel
	case "error":
		return zerolog.ErrorLevel
	case "warn":
		return zerolog.WarnLevel
	case "info":
		return zerolog.InfoLevel
	case "debug":
		return zerolog.DebugLevel
	case "trace":
		return zerolog.TraceLevel
	}
	return zerolog.TraceLevel
}

type S3 struct {
	AccessKeyId     string `toml:"access_key_id"`
	SecretAccessKey string `toml:"secret_access_key"`
	Bucket          string `toml:"bucket"`
	Region          string `toml:"region"`
	Endpoint        string `toml:"endpoint"`
}

type SQS struct {
	AccessKeyId     string `toml:"access_key_id"`
	SecretAccessKey string `toml:"secret_access_key"`
	SqsURL          string `toml:"sqs_url"`
	Region          string `toml:"region"`
	Endpoint        string `toml:"endpoint"`
}

type API struct {
	Enabled bool   `toml:"enabled"`
	Port    int    `toml:"port"`
	DataDir string `toml:"data"`

	Readonly bool `tom:"readonly"`

	// How often to rotate log file
	MaxAgeSeconds int `toml:"max_age_seconds"`

	// Max file size before rotating
	MaxSizeBytes int64 `toml:"max_size_bytes"`

	HealthCheckPath        string `toml:"health_check_path"`
	FreeSpaceRequiredBytes int64  `toml:"free_space_required_bytes"`
}

type Transport struct {
	Type            string `toml:"type"`
	Workers         int    `toml:"workers"`
	ProducerEnabled bool   `toml:"producer_enabled"`
	ConsumerEnabled bool   `toml:"consumer_enabled"`

	QueueStorage QueueStorage `toml:"queue_storage"`
}

type QueueStorage struct {
	ProducerDataDir        string `toml:"producer_data_dir"`
	ConsumerDataDir        string `toml:"consumer_data_dir"`
	MaxFileSizeBytes       int64  `toml:"max_file_size_bytes"`
	MaxRows                int64  `toml:"max_rows"`
	MaxFileAgeSeconds      int    `toml:"max_file_age_seconds"`
	FreeSpaceRequiredBytes uint64 `toml:"free_space_required_bytes"`
	DequeueTimeoutSeconds  int    `toml:"dequeue_timeout_seconds"`
}
