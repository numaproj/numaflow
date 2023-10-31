package authz

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"

	"github.com/numaproj/numaflow/server/authn"
)

type Authorizer interface {
	// Authorize checks if a user is authorized to access the resource.
	// c is the gin context.
	// g is the list of groups the user belongs to.
	// scope is the scope of the authorization.
	// Authorize trusts that the user is already authenticated and directly uses the groups to authorize the user.
	// please don't use gin to get the user information again.
	// Authorize returns true if the user is authorized, otherwise false.
	// Authorize also returns the policy count of the user. The policy count is used to check if there are any policies
	// defined for the given user, if not we will allocate a default policy for the user.
	Authorize(c *gin.Context, userIdentityToken *authn.UserInfo, scope string) (bool, int)
	// GetConfig returns the config file of the authorizer. We use a config file to store the policy params
	// so that we can change the policy without restarting the server. The config file is in the format of yaml.
	// The config file is read by viper.
	GetConfig() *viper.Viper
	// GetScopes returns the scopes of the authorizer. The scopes are used to check the authentication params to check
	// if the user is authorized to access the resource.
	GetScopes() []string
}
