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
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/common"
)

type noAuthHandler struct {
	dexObj               *DexObject
	localUsersAuthObject *LocalUsersAuthObject
}

type LoginInput struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// NewNoAuthHandler is used to provide a new instance of the handler type
func NewNoAuthHandler(dexObj *DexObject, localUsersAuthObject *LocalUsersAuthObject) (*noAuthHandler, error) {
	return &noAuthHandler{
		dexObj:               dexObj,
		localUsersAuthObject: localUsersAuthObject,
	}, nil
}

// Login is used to generate the authentication URL and return the URL as part of the return payload.
func (h *noAuthHandler) Login(c *gin.Context) {
	h.dexObj.handleLogin(c)
}

// LoginLocalUsers is used to generate the jwt token and return it as part of the return payload.
// The jwt token and login type are also set as a cookie.
func (h *noAuthHandler) LoginLocalUsers(c *gin.Context) {
	if h.localUsersAuthObject.authDisabled {
		errMsg := "Auth is disabled"
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	var (
		input LoginInput
		err   error
	)

	if err = c.ShouldBindJSON(&input); err != nil {
		errMsg := "Missing input fields"
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	user := authn.LoginCredentials{}
	user.Username = input.Username
	user.Password = input.Password

	// Verify user for the given username and password
	if err = h.localUsersAuthObject.VerifyUser(c, user.Username, user.Password); err != nil {
		errMsg := fmt.Sprintf("%v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	//GenerateToken
	token, err := h.localUsersAuthObject.GenerateToken(c, user.Username)
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

// Callback is used to extract user authentication information from the Dex Server returned payload.
func (h *noAuthHandler) Callback(c *gin.Context) {
	h.dexObj.handleCallback(c)
}

// Logout is used to remove auth cookie ending a user's session.
func (h *noAuthHandler) Logout(c *gin.Context) {
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

	if loginString == "dex" {
		cookies := c.Request.Cookies()
		tokenString, err := common.JoinCookies(common.UserIdentityCookieName, cookies)
		var invalidCookieE *common.InvalidCookieError
		if errors.As(err, &invalidCookieE) {
			errMsg := fmt.Sprintf("Failed to retrieve user identity token: %v", err)
			c.JSON(http.StatusBadRequest, NewNumaflowAPIResponse(&errMsg, nil))
			return
		}
		if tokenString == "" {
			// no numaflow.token found, return directly
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
			return
		}
		// if there's any numaflow.token cookie, delete
		for _, cookie := range cookies {
			if !strings.HasPrefix(cookie.Name, common.UserIdentityCookieName) {
				continue
			}
			c.SetCookie(cookie.Name, "", -1, "/", "", true, true)
		}
	} else if loginString == "local" {
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
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}
