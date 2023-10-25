package utils

import (
	"encoding/json"
	"fmt"

	v1 "github.com/numaproj/numaflow/server/apis/v1"
)

// ParseUserIdentityToken is used to extract user identity token from the Dex Server returned payload.
func ParseUserIdentityToken(jsonStr string) (*v1.UserIdInfo, error) {
	var callbackResponse *v1.UserIdInfo
	err := json.Unmarshal([]byte(jsonStr), callbackResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal user identity token: %v", err)
	}
	return callbackResponse, nil
}
