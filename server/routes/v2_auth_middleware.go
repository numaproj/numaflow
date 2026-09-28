/*
Copyright 2026 The Numaproj Authors.

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

	v2 "github.com/numaproj/numaflow/server/apis/v2"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/authz"
)

// v2AuthMiddleware authenticates existing browser cookie sessions and applies
// route-specific authorization while returning API v2 Problem responses on failure.
// Bearer-token support is intentionally deferred to a later API v2 auth change.
func v2AuthMiddleware(ctx context.Context, authorizer authz.Authorizer, dexAuthenticator authn.Authenticator, localUsersAuthenticator authn.Authenticator, authRouteMap authz.RouteMap) gin.HandlerFunc {
	return authenticationAuthorizationMiddleware(
		ctx,
		authorizer,
		dexAuthenticator,
		localUsersAuthenticator,
		authRouteMap,
		func(c *gin.Context, failure authFailure) {
			status := http.StatusUnauthorized
			code := "authentication_failed"
			title := "Request authentication failed"
			if failure.kind == authFailureMissingRoute {
				status = http.StatusForbidden
				code = "route_not_authorized"
				title = "Request authorization failed"
			}
			if failure.kind == authFailureAuthorizationDenied {
				status = http.StatusForbidden
				code = "authorization_denied"
				title = "Request authorization failed"
			}
			v2.WriteProblem(c, status, code, title, failure.v2Detail(), nil)
		},
	)
}
