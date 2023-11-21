package config

type Config struct {
	Ingest            IngestConfig       `mapstructure:"ingest"`
	Insert            InsertConfig       `mapstructure:"insert"`
	AWS               AWS                `mapstructure:"aws"`
	SSL               SSL                `mapstructure:"ssl"`
	Storage           Storage            `mapstructure:"storage"`
	ClickhouseServers []ClickhouseConfig `mapstructure:"clickhouse"`
	Users             []UserConfig       `mapstructure:"users"`
}

type UserConfig struct {
	Name           string `mapstructure:"name" toml:"name"`
	APIKey         string `mapstructure:"api_key" toml:"api_key"`
	DBCluster      string `mapstructure:"db_cluster" toml:"db_cluster"`
	DBShardMacro   string `mapstructure:"db_shard_macro" toml:"db_shard_macro"`
	DBReplicaMacro string `mapstructure:"db_replica_macro" toml:"db_replica_macro"`
	DBName         string `mapstructure:"db_name" toml:"db_name"`
	DBUser         string `mapstructure:"db_user" toml:"db_user"`
	DBPassword     string `mapstructure:"db_password" toml:"db_password"`
}

type ClickhouseConfig struct {
	HTTPProtocol string `mapstructure:"protocol"`
	Host         string `mapstructure:"host"`
	HTTPPort     int    `mapstructure:"http_port"`
	TCPPort      int    `mapstructure:"tcp_port"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`

	StoragePolicy string `mapstructure:"storage_policy"`

	MaxOpenConns        int  `mapstructure:"max_open_conns"`
	MaxIdleConns        int  `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSecs int  `mapstructure:"conn_max_lifetime_secs"`
	TLS                 bool `mapstructure:"tls"`

	HostedAPIKeys  []string `mapstructure:"hosted_api_keys"`
	HostedClusters []string `mapstructure:"hosted_clusters"`
	HostedDBs      []string `mapstructure:"hosted_databases"`
}

type InsertConfig struct {
	Enabled                bool   `mapstructure:"enabled"` // Not used
	Workers                int    `mapstructure:"workers"`
	SleepSeconds           int    `mapstructure:"sleep_seconds"`
	DataDir                string `mapstructure:"data"`
	FreeSpaceRequiredBytes int64  `mapstructure:"free_space_required_bytes"`
	MaxOpenConns           int    `mapstructure:"max_open_conns"`
	MaxIdleConns           int    `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSecs    int    `mapstructure:"conn_max_lifetime"`
}

type IngestConfig struct {
	Enabled bool   `mapstructure:"enabled"` // Not used
	Port    string `mapstructure:"port"`
	DataDir string `mapstructure:"data"`

	// How often to rotate log file
	MaxAgeSeconds int `mapstructure:"max_age_seconds"`

	// Max file size before rotating
	MaxSizeBytes int64 `mapstructure:"max_size_bytes"`

	HealthCheckPath        string `mapstructure:"health_check_path"`
	FreeSpaceRequiredBytes int64  `mapstructure:"free_space_required_bytes"`
}

type AWS struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	SQS             string `mapstructure:"sqs"`
	Region          string `mapstructure:"region"`
	Endpoint        string `mapstructure:"endpoint"`
}

type Storage struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	S3Bucket        string `mapstructure:"s3_bucket"`
	Region          string `mapstructure:"region"`
	Endpoint        string `mapstructure:"endpoint"`
}

type SSL struct {
	Enabled   bool     `mapstructure:"enabled"`
	Hostnames []string `mapstructure:"hostnames"`
}
