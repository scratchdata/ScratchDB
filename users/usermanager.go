package users

import "github.com/scratchdata/scratchdb/servers"

type UserManager interface {
	AddUser(name string) error
	GetDBManager() servers.ClickhouseManager
}
