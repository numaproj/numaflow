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
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	v2 "github.com/numaproj/numaflow/server/apis/v2"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/authz"
	"github.com/numaproj/numaflow/server/common"
)

// v2AuthMiddleware authenticates existing browser cookie sessions and applies
// route-specific authorization while returning API v2 Problem responses on failure.
// Bearer-token support is intentionally deferred to a later API v2 auth change.
func v2AuthMiddleware(ctx context.Context, authorizer authz.Authorizer, dexAuthenticator authn.Authenticator, localUsersAuthenticator authn.Authenticator, authRouteMap authz.RouteMap) gin.HandlerFunc {
	return func(c *gin.Context) {
		log := logging.FromContext(ctx)
		loginType, err := c.Cookie(common.LoginCookieName)
		if err != nil {
			v2.WriteProblem(c, http.StatusUnauthorized, "authentication_failed", "Request authentication failed", fmt.Sprintf("failed to get login type: %v", err), nil)
			return
		}

		var userInfo *authn.UserInfo
		switch loginType {
		case "dex":
			userInfo, err = dexAuthenticator.Authenticate(c)
		case "local":
			userInfo, err = localUsersAuthenticator.Authenticate(c)
		default:
			v2.WriteProblem(c, http.StatusUnauthorized, "authentication_failed", "Request authentication failed", fmt.Sprintf("unidentified login type received: %v", loginType), nil)
			return
		}
		if err != nil {
			v2.WriteProblem(c, http.StatusUnauthorized, "authentication_failed", "Request authentication failed", fmt.Sprintf("failed to authenticate user: %v", err), nil)
			return
		}

		// The OpenAPI-derived map determines whether this authenticated route also
		// requires a Casbin authorization decision.
		routeInfo := authRouteMap.GetRouteFromContext(c)
		if routeInfo == nil {
			log.Errorw("route not present in routeMap", "route", authz.GetRouteMapKey(c))
			v2.WriteProblem(c, http.StatusForbidden, "route_not_authorized", "Request authorization failed", "Invalid route", nil)
			return
		}
		if routeInfo.RequiresAuthZ && !authorizer.Authorize(c, userInfo) {
			v2.WriteProblem(c, http.StatusForbidden, "authorization_denied", "Request authorization failed", "user is not authorized to execute the requested action", nil)
			return
		}
		c.Next()
	}
}
