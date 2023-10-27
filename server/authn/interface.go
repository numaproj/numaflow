package authn

import "github.com/gin-gonic/gin"

type Authenticator interface {
	// Authenticate is used to validate the user's identity.
	// If the user is authenticated, the function returns user information.
	// Otherwise, empty information with the corresponding error.
	Authenticate(c *gin.Context) (*UserInfo, error)
}
