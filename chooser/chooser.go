package chooser

import (
	"github.com/scratchdata/scratchdb/apikeys"
	"github.com/scratchdata/scratchdb/servers"
)

type ServerChooser interface {
	ChooseServerForWriting(servers.ClickhouseManager, apikeys.APIKeyDetails) (servers.ClickhouseServer, error)
	ChooseServerForReading(servers.ClickhouseManager, apikeys.APIKeyDetails) (servers.ClickhouseServer, error)
}
