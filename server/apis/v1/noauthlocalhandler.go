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

package v1

import (
	"fmt"
	"github.com/numaproj/numaflow/server/authn"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/server/common"
)

type noAuthLocalHandler struct {
	localAuthObject *LocalAuthObject
}

// NewNoAuthLocalHandler is used to provide a new instance of the handler type
func NewNoAuthLocalHandler(localAuthObject *LocalAuthObject) (*noAuthLocalHandler, error) {
	return &noAuthLocalHandler{
		localAuthObject: localAuthObject,
	}, nil
}

type LoginInput struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// Login is used to generate the jwt token and return it as part of the return payload.
// The jwt token and login type are also set as a cookie.
func (h *noAuthLocalHandler) Login(c *gin.Context) {
	if h.localAuthObject.authDisabled {
		errMsg := fmt.Sprintf("Auth is disabled")
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	var (
		input LoginInput
		err   error
	)

	if err = c.ShouldBindJSON(&input); err != nil {
		errMsg := fmt.Sprintf("Missing input fields")
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	user := authn.User{}
	user.Username = input.Username
	user.Password = input.Password

	// Verify user for the given username and password
	if err = h.localAuthObject.VerifyUser(c, user.Username, user.Password); err != nil {
		errMsg := fmt.Sprintf("%v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	//GenerateToken
	token, err := h.localAuthObject.GenerateToken(c, user.Username)
	if err != nil {
		errMsg := fmt.Sprintf("Error while generating user identity token: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	if token == "" {
		errMsg := "Failed to get token: empty user identity Token"
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	// Set the login type as a cookie
	c.SetCookie(common.LoginCookieName, "local", common.JWTCookieMaxAge, "/", "", true, true)
	// Set the JWT token as a cookie
	c.SetCookie(common.JWTCookieName, token, common.JWTCookieMaxAge, "/", "", true, true)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, token))
}

// Logout is used to remove auth cookies ending a user's session.
func (h *noAuthLocalHandler) Logout(c *gin.Context) {
	tokenString, err := c.Cookie(common.JWTCookieName)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to retrieve user identity token: %v", err)
		c.JSON(http.StatusBadRequest, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	if tokenString == "" {
		// no jwt found, return directly
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}
	// clearing jwt cookie
	c.SetCookie(common.JWTCookieName, "", -1, "/", "", true, true)

	loginString, err := c.Cookie(common.LoginCookieName)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to retrieve login type cookie: %v", err)
		c.JSON(http.StatusBadRequest, NewNumaflowAPIResponse(&errMsg, nil))
	}
	if loginString == "" {
		// no numaflow-login found, return directly
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}
	// clearing numaflow-login cookie
	c.SetCookie(common.LoginCookieName, "", -1, "/", "", true, true)

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}
