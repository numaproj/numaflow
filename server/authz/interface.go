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

package authz

import (
	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/server/authn"
)

type Authorizer interface {
	// Authorize checks if a user is authorized to access the resource.
	// Authorize trusts that the user is already authenticated and directly uses the groups to authorize the user.
	// please don't use gin to get the user information again.
	// Authorize returns true if the user is authorized, otherwise false.
	Authorize(c *gin.Context, userInfo *authn.UserInfo) bool
}
