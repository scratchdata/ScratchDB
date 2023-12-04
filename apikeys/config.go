package apikeys

import (
	"errors"
	"scratchdb/config"
)

type APIKeysFromConfig struct {
	Users []config.UserConfig
}

type APIKeyDetailsFromConfig struct {
	user *config.UserConfig
}

func (k *APIKeysFromConfig) Healthy() error {
	if k.Users == nil {
		return errors.New(("Users is null"))
	}

	if len(k.Users) == 0 {
		return errors.New(("Users is empty"))
	}

	return nil
}

func (k *APIKeysFromConfig) GetDetailsByKey(key string) (APIKeyDetails, bool) {
	for _, u := range k.Users {
		if u.APIKey == key {
			return &APIKeyDetailsFromConfig{user: &u}, true
		}

	}
	return nil, false
}

func (k *APIKeysFromConfig) CreateKey(APIKeyDetails) (APIKeyDetails, error) {
	return &APIKeyDetailsFromConfig{}, nil
}

func (k *APIKeysFromConfig) DeleteKey(key string) error {
	return errors.New("Unsupported")
}

func (k *APIKeyDetailsFromConfig) StringOnlyMode() bool {
	return k.user.StringOnlyMode
}

func (k *APIKeyDetailsFromConfig) GetAPIKey() string {
	return k.user.APIKey
}

func (k *APIKeyDetailsFromConfig) GetName() string {
	return k.user.Name
}

func (k *APIKeyDetailsFromConfig) GetDBCluster() string {
	return k.user.DBCluster
}

func (k *APIKeyDetailsFromConfig) GetDBName() string {
	return k.user.DBName
}

func (k *APIKeyDetailsFromConfig) GetDBUser() string {
	return k.user.DBUser
}

func (k *APIKeyDetailsFromConfig) GetDBPassword() string {
	return k.user.DBPassword
}

func (k *APIKeyDetailsFromConfig) GetPermissions() APIKeyPermissions {
	return APIKeyPermissions{}
}
