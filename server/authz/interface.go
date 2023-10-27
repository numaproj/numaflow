package authz

import (
	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/server/authn"
)

type Authorizer interface {
	// Authorize checks if a user is authorized to access the resource.
	// c is the gin context.
	// g is the list of groups the user belongs to.
	// scope is the scope of the authorization.
	// Authorize trusts that the user is already authenticated and directly uses the groups to authorize the user.
	// please don't use gin to get the user information again.
	Authorize(c *gin.Context, userIdentityToken *authn.UserInfo, scope string) bool
}
