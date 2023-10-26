/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
