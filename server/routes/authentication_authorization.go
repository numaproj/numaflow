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

	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/authz"
	"github.com/numaproj/numaflow/server/common"
)

type authFailureKind int

const (
	authFailureMissingLoginCookie authFailureKind = iota
	authFailureUnknownLoginType
	authFailureAuthentication
	authFailureMissingRoute
	authFailureAuthorizationDenied
)

type authFailure struct {
	kind      authFailureKind
	err       error
	loginType string
}

type authFailureWriter func(*gin.Context, authFailure)

func authenticationAuthorizationMiddleware(
	ctx context.Context,
	authorizer authz.Authorizer,
	dexAuthenticator authn.Authenticator,
	localUsersAuthenticator authn.Authenticator,
	authRouteMap authz.RouteMap,
	writeFailure authFailureWriter,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		if failure := authenticateAndAuthorize(
			ctx,
			c,
			authorizer,
			dexAuthenticator,
			localUsersAuthenticator,
			authRouteMap,
		); failure != nil {
			writeFailure(c, *failure)
			c.Abort()
			return
		}
		c.Next()
	}
}

func authenticateAndAuthorize(
	ctx context.Context,
	c *gin.Context,
	authorizer authz.Authorizer,
	dexAuthenticator authn.Authenticator,
	localUsersAuthenticator authn.Authenticator,
	authRouteMap authz.RouteMap,
) *authFailure {
	loginType, err := c.Cookie(common.LoginCookieName)
	if err != nil {
		return &authFailure{kind: authFailureMissingLoginCookie, err: err}
	}

	var userInfo *authn.UserInfo
	switch loginType {
	case "dex":
		userInfo, err = dexAuthenticator.Authenticate(c)
	case "local":
		userInfo, err = localUsersAuthenticator.Authenticate(c)
	default:
		return &authFailure{kind: authFailureUnknownLoginType, loginType: loginType}
	}
	if err != nil {
		return &authFailure{kind: authFailureAuthentication, err: err}
	}

	routeInfo := authRouteMap.GetRouteFromContext(c)
	if routeInfo == nil {
		logging.FromContext(ctx).Errorw("route not present in routeMap", "route", authz.GetRouteMapKey(c))
		return &authFailure{kind: authFailureMissingRoute}
	}
	if routeInfo.RequiresAuthZ && !authorizer.Authorize(c, userInfo) {
		return &authFailure{kind: authFailureAuthorizationDenied}
	}
	return nil
}

func (f authFailure) v1Message() string {
	switch f.kind {
	case authFailureMissingLoginCookie:
		return fmt.Sprintf("Failed to get login type: %v", f.err)
	case authFailureUnknownLoginType:
		return fmt.Sprintf("unidentified login type received: %v", f.loginType)
	case authFailureAuthentication:
		return fmt.Sprintf("Failed to authenticate user: %v", f.err)
	case authFailureMissingRoute:
		return "Invalid route"
	case authFailureAuthorizationDenied:
		return "user is not authorized to execute the requested action"
	default:
		return "authentication failed"
	}
}

func (f authFailure) v2Detail() string {
	switch f.kind {
	case authFailureMissingLoginCookie:
		return fmt.Sprintf("failed to get login type: %v", f.err)
	case authFailureUnknownLoginType:
		return fmt.Sprintf("unidentified login type received: %v", f.loginType)
	case authFailureAuthentication:
		return fmt.Sprintf("failed to authenticate user: %v", f.err)
	case authFailureMissingRoute:
		return "Invalid route"
	case authFailureAuthorizationDenied:
		return "user is not authorized to execute the requested action"
	default:
		return "authentication failed"
	}
}
