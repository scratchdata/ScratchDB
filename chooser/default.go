package chooser

import (
	"errors"

	"github.com/scratchdata/scratchdb/apikeys"
	"github.com/scratchdata/scratchdb/servers"
)

type DefaultChooser struct{}

func (c *DefaultChooser) chooseFirstServer(serverManager servers.ClickhouseManager, userManager apikeys.APIKeyDetails) (servers.ClickhouseServer, error) {
	var eligibleDBServers []servers.ClickhouseServer
	if userManager.GetDBCluster() != "" {
		eligibleDBServers = serverManager.GetServersByDBCluster(userManager.GetDBCluster())
	} else {
		eligibleDBServers = serverManager.GetServersByDBName(userManager.GetDBName())
	}

	if eligibleDBServers == nil || len(eligibleDBServers) == 0 {
		return nil, errors.New("Unable to find eligible server to query")
	}

	return eligibleDBServers[0], nil
}

func (c *DefaultChooser) ChooseServerForWriting(serverManager servers.ClickhouseManager, userManager apikeys.APIKeyDetails) (servers.ClickhouseServer, error) {
	return c.chooseFirstServer(serverManager, userManager)
}

func (c *DefaultChooser) ChooseServerForReading(serverManager servers.ClickhouseManager, userManager apikeys.APIKeyDetails) (servers.ClickhouseServer, error) {
	return c.chooseFirstServer(serverManager, userManager)
}
