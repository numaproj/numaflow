package serverinfo

import (
	"github.com/numaproj/numaflow-go/pkg/info"
)

type sdkConstraints map[info.Language]string

var minimumSupportedSDKVersions = sdkConstraints{
	info.Go:     "0.7.0-rc2",
	info.Python: "0.7.0a1",
	info.Java:   "0.7.2-0",
}
