package authn

import (
	"context"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/gin-gonic/gin"
)

type Authenticator interface {
	HandleLogin(c *gin.Context)
	HandleCallback(c *gin.Context)
	HandleLogout(c *gin.Context)
	Authenticate(c *gin.Context) (string, error)
	Verify(ctx context.Context, rawIDToken string) (*oidc.IDToken, error)
}
