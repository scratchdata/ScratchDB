package apikeys

type APIKeys interface {
	Healthy() error
	GetDetailsByKey(key string) (APIKeyDetails, bool)
	CreateKey(APIKeyDetails) (APIKeyDetails, error)
	DeleteKey(key string) error
}

type APIKeyDetails interface {
	GetAPIKey() string
	GetName() string
	GetDBCluster() string
	GetDBShardMacro() string
	GetDBReplicaMacro() string
	GetDBUser() string
	GetDBName() string
	GetDBPassword() string
	GetPermissions() APIKeyPermissions
}

type APIKeyPermissions struct {
	// User      string
	// CanRead   bool
	// CanWrite  bool
	// CanAdmin  bool
	// Databases []string
	// Tables    []string
}
