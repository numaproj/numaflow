package authz

import "github.com/gin-gonic/gin"

type Authorizer interface {
	// Authorize is used to authorize the user's access to the resource.
	Authorize(c *gin.Context) (bool, error)
}
