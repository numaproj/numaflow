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

package routes

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	v1 "github.com/numaproj/numaflow/server/apis/v1"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/authz"
)

// authMiddleware is the middleware for AuthN/AuthZ.
// it ensures the user is authenticated and authorized
// to execute the requested action before sending the request to the api handler.
func authMiddleware(ctx context.Context, authorizer authz.Authorizer, dexAuthenticator authn.Authenticator, localUsersAuthenticator authn.Authenticator, authRouteMap authz.RouteMap) gin.HandlerFunc {
	return authenticationAuthorizationMiddleware(
		ctx,
		authorizer,
		dexAuthenticator,
		localUsersAuthenticator,
		authRouteMap,
		func(c *gin.Context, failure authFailure) {
			errMsg := failure.v1Message()
			status := http.StatusUnauthorized
			if failure.kind == authFailureMissingRoute || failure.kind == authFailureAuthorizationDenied {
				status = http.StatusForbidden
			}
			c.JSON(status, v1.NewNumaflowAPIResponse(&errMsg, nil))
		},
	)
}
