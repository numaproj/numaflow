package sdkserverinfo

import (
	"log"

	"github.com/numaproj/numaflow-go/pkg/info"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// SDKServerInfo wait for the server to start and return the server info
func SDKServerInfo(opts *sdkclient.Options) (*info.ServerInfo, error) {
	serverInfo, err := util.WaitForServerInfo(opts.ServerInfoReadinessTimeout(), opts.ServerInfoFilePath())
	if err != nil {
		return nil, err
	}
	if serverInfo != nil {
		log.Printf("ServerInfo: %v\n", serverInfo)
	}
	return serverInfo, nil
}
