package v1

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/server/common"
)

type noAuthHandler struct {
	dexObj *DexObject
}

// NewNoAuthHandler is used to provide a new instance of the handler type
func NewNoAuthHandler(serverAddr string, proxyAddr string) (*noAuthHandler, error) {
	dexObj, err := NewDexObject(serverAddr, proxyAddr)
	if err != nil {
		return nil, err
	}
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
	c.SetCookie(common.UserIdentityCookieName, "", -1, "/", "", true, true)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}
