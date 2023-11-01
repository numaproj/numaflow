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
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/server/common"
)

type noAuthHandler struct {
	dexObj *DexObject
}

// NewNoAuthHandler is used to provide a new instance of the handler type
func NewNoAuthHandler(dexObj *DexObject) (*noAuthHandler, error) {
	return &noAuthHandler{
		dexObj: dexObj,
	}, nil
}

// Login is used to generate the authentication URL and return the URL as part of the return payload.
func (h *noAuthHandler) Login(c *gin.Context) {
	h.dexObj.handleLogin(c)
}

// Callback is used to extract user authentication information from the Dex Server returned payload.
func (h *noAuthHandler) Callback(c *gin.Context) {
	h.dexObj.handleCallback(c)
}

// Logout is used to remove auth cookie ending a user's session.
func (h *noAuthHandler) Logout(c *gin.Context) {
	cookies := c.Request.Cookies()
	tokenString, _ := common.JoinCookies(common.UserIdentityCookieName, cookies)
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
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}
