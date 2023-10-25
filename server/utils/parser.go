package utils

import (
	"encoding/json"
	"fmt"

	"github.com/numaproj/numaflow/server/authn"
)

// ParseUserIdentityToken is used to extract user identity token from the Dex Server returned payload.
func ParseUserIdentityToken(jsonStr string) (authn.UserIdInfo, error) {
	var u authn.UserIdInfo
	err := json.Unmarshal([]byte(jsonStr), &u)
	if err != nil {
		return authn.UserIdInfo{}, fmt.Errorf("failed to unmarshal user identity token: %v", err)
	}
	return u, nil
}
