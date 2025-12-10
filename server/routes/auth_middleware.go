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
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	v1 "github.com/numaproj/numaflow/server/apis/v1"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/authz"
	"github.com/numaproj/numaflow/server/common"
)

// authMiddleware is the middleware for AuthN/AuthZ.
// it ensures the user is authenticated and authorized
// to execute the requested action before sending the request to the api handler.
func authMiddleware(ctx context.Context, authorizer authz.Authorizer, dexAuthenticator authn.Authenticator, localUsersAuthenticator authn.Authenticator, authRouteMap authz.RouteMap) gin.HandlerFunc {

	return func(c *gin.Context) {

		log := logging.FromContext(ctx)
		var userInfo *authn.UserInfo

		loginType, err := c.Cookie(common.LoginCookieName)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get login type: %v", err)
			c.JSON(http.StatusUnauthorized, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
			return
		}

		// Authenticate the user based on the login type.
		switch loginType {
		case "dex":
			userInfo, err = dexAuthenticator.Authenticate(c)
		case "local":
			userInfo, err = localUsersAuthenticator.Authenticate(c)
		default:
			errMsg := fmt.Sprintf("unidentified login type received: %v", loginType)
			c.JSON(http.StatusUnauthorized, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
			return
		}
		if err != nil {
			errMsg := fmt.Sprintf("Failed to authenticate user: %v", err)
			c.JSON(http.StatusUnauthorized, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
			return
		}
		// Check if the route requires authorization.
		if authRouteMap.GetRouteFromContext(c) != nil && authRouteMap.GetRouteFromContext(c).RequiresAuthZ {
			// Check if the user is authorized to execute the requested action.
			isAuthorized := authorizer.Authorize(c, userInfo)
			if isAuthorized {
				// If the user is authorized, continue the request.
				c.Next()
			} else {
				// If the user is not authorized, return an error.
				errMsg := "user is not authorized to execute the requested action"
				c.JSON(http.StatusForbidden, v1.NewNumaflowAPIResponse(&errMsg, nil))
				c.Abort()
			}
		} else if authRouteMap.GetRouteFromContext(c) != nil && !authRouteMap.GetRouteFromContext(c).RequiresAuthZ {
			// If the route does not require AuthZ, skip the AuthZ check.
			c.Next()
		} else {
			// If the route is not present in the route map, return an error.
			log.Errorw("route not present in routeMap", "route", authz.GetRouteMapKey(c))
			errMsg := "Invalid route"
			c.JSON(http.StatusForbidden, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
		}
	}
}
