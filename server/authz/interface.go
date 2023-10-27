package authz

import "github.com/gin-gonic/gin"

type Authorizer interface {
	// Authorize checks if a user is authorized to access the resource.
	// c is the gin context.
	// g is the list of groups the user belongs to.
	// Authorize trusts that the user is already authenticated and directly uses the groups to authorize the user.
	// please don't use gin to get the user information again.
	Authorize(c *gin.Context, g []string) (bool, error)
}
