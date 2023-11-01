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

package authz

import (
	_ "embed"
	"fmt"
	"path"
	"strings"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/fsnotify/fsnotify"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"k8s.io/utils/strings/slices"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/server/authn"
)

var (
	//go:embed rbac-model.conf
	rbacModel     string
	logger        = logging.NewLogger()
	userPermCount map[string]int
	currentScopes []string
	policyDefault string
	configReader  *viper.Viper
)

const (
	emptyString = ""
)

type CasbinObject struct {
	enforcer *casbin.Enforcer
}

func NewCasbinObject() (*CasbinObject, error) {
	enforcer, err := getEnforcer()
	if err != nil {
		return nil, err
	}
	configReader = viper.New()
	configReader.SetConfigFile(rbacPropertiesPath)
	err = configReader.ReadInConfig()
	currentScopes = getRBACScopes(configReader)
	if err != nil {
		return nil, err
	}
	// Watch for changes in the config file.
	configReader.WatchConfig()
	configReader.OnConfigChange(func(in fsnotify.Event) {
		ConfigFileReload(in)
	})
	// Set the default policy for authorization.
	policyDefault = getDefaultPolicy(configReader)

	return &CasbinObject{
		enforcer: enforcer,
	}, nil
}

// Authorize checks if a user is authorized to access the resource.
// It returns true if the user is authorized, otherwise false.
// It also returns the policy count of the user. The policy count is used to check if there are any policies defined
// for the given user, if not we will allocate a default policy for the user.
func (cas *CasbinObject) Authorize(c *gin.Context, userIdentityToken *authn.UserInfo) bool {
	// Get the scopes to check from the policy.
	scopedList := getSubjectFromScope(currentScopes, userIdentityToken)
	// Get the resource, object and action from the request.
	resource := extractResource(c)
	object := extractObject(c)
	action := c.Request.Method
	userHasPolicies := false
	// Check for the given scoped list if the user is authorized using any of the subjects in the list.
	for _, scopedSubject := range scopedList {
		// Check if the user has permissions in the policy for the given scoped subject.
		userHasPolicies = userHasPolicies || getPermissionCount(cas.enforcer, scopedSubject)
		ok := enforceCheck(cas.enforcer, scopedSubject, resource, object, action)
		if ok {
			return ok
		}
	}
	// If the user does not have any policy defined, allocate a default policy for the user.
	if !userHasPolicies {
		logger.Infow("No policy defined for the user, allocating default policy",
			"DefaultPolicy", policyDefault)
		ok := enforceCheck(cas.enforcer, policyDefault, resource, object, action)
		if ok {
			return ok
		}
	}
	return false
}

// getSubjectFromScope returns the subjects in the request for the given scopes.
// The scopes are the params used to check the authentication params to check if the
// user is authorized to access the resource. For any new scope, add the scope to the
// rbac properties file and add the scope to the cases below.
func getSubjectFromScope(scopes []string, userIdentityToken *authn.UserInfo) []string {
	var scopedList []string
	// If the scope is group, fetch the groups from the user identity token.
	if slices.Contains(scopes, ScopeGroup) {
		for _, group := range userIdentityToken.IDTokenClaims.Groups {
			scopedList = append(scopedList, group)
		}
	}
	// If the scope is email, fetch the email from the user identity token.
	if slices.Contains(scopes, ScopeEmail) {
		scopedList = append(scopedList, userIdentityToken.IDTokenClaims.Email)
	}
	return scopedList
}

// getEnforcer initializes the Casbin Enforcer with the model and policy.
func getEnforcer() (*casbin.Enforcer, error) {
	modelRBAC, err := model.NewModelFromString(rbacModel)
	if err != nil {
		return nil, err
	}
	a := fileadapter.NewAdapter(policyMapPath)

	// Initialize the Casbin Enforcer with the model and policies.
	enforcer, err := casbin.NewEnforcer(modelRBAC, a)
	if err != nil {
		return nil, err
	}
	enforcer.AddFunction("patternMatch", patternMatch)
	enforcer.AddFunction("stringMatch", stringMatch)
	return enforcer, nil
}

// patternMatch is used to match namespaces from the policy, if * is provided it will match all namespaces.
// Otherwise, we will enforce based on the namespace provided.
func patternMatch(args ...interface{}) (interface{}, error) {
	req, policy, err := extractArgs(args)
	if err != nil {
		return false, err
	}
	// If policy is MatchAll, allow all the strings.
	if policy == MatchAll {
		return true, nil
	}
	// pattern, namespace
	return path.Match(policy, req)
}

// stringMatch is used to match strings from the policy, if * is provided it will match all namespaces.
// Otherwise, we will enforce based on the namespace provided.
func stringMatch(args ...interface{}) (interface{}, error) {
	req, policy, err := extractArgs(args)
	if err != nil {
		return false, err
	}

	// If policy is MatchAll, allow all the strings.
	if policy == MatchAll {
		return true, nil
	}
	// pattern, namespace
	return policy == req, nil
}

// extractArgs extracts the arguments from the Casbin policy.
func extractArgs(args ...interface{}) (string, string, error) {
	// Should be 2 arguments as a list
	args = args[0].([]interface{})
	if len(args) != 2 {
		return emptyString, emptyString, fmt.Errorf("expected 2 arguments, got %d", len(args))
	}
	req, ok := args[0].(string)
	if !ok {
		return emptyString, emptyString, fmt.Errorf("expected first argument to be string, got %T", args[0])
	}
	policy, ok := args[1].(string)
	if !ok {
		return emptyString, emptyString, fmt.Errorf("expected second argument to be string, got %T", args[1])
	}
	return req, policy, nil
}

// extractResource extracts the resource from the request.
func extractResource(c *gin.Context) string {
	// We use the namespace in the request as the resource.
	resource := c.Param(ResourceNamespace)
	if resource == emptyString {
		return emptyString
	}
	return resource
}

// extractObject extracts the object from the request.
func extractObject(c *gin.Context) string {
	action := c.Request.Method
	// Get the route map from the context. Key is in the format "method:path".
	routeMapKey := fmt.Sprintf("%s:%s", action, c.FullPath())
	// Return the object from the route map.
	if RouteMap[routeMapKey] != nil {
		return RouteMap[routeMapKey].Object
	}
	return emptyString
}

// getRbacProperty is used to read the rbacPropertiesPath file path and extract the policy provided as argument,
func getRbacProperty(property string, config *viper.Viper) interface{} {
	val := config.Get(property)
	if val == nil {
		return emptyString
	}
	return val
}

// getRBACScopes returns the scopes from the rbac properties file. If no scopes are provided, it returns Group as the
// default scope. The scopes are used to determine the user identity token to be used for authorization.
// If the scope is group, the user identity token will be the groups assigned to the user from the authentication
// system. If the scope is email, the user identity token will be the email assigned to
// the user from the authentication.
// The scopes are provided as a comma separated list in the rbac properties file.
// Example: policy.scopes=groups,email
func getRBACScopes(config *viper.Viper) []string {
	scopes := getRbacProperty(RbacPropertyScopes, config)
	var retList []string
	// If no scopes are provided, set Group as the default scope.
	if scopes == emptyString {
		retList = append(retList, ScopeGroup)
		return retList
	}
	scopes = strings.Split(scopes.(string), ",")
	for _, scope := range scopes.([]string) {
		scope = strings.TrimSpace(scope)
		retList = append(retList, scope)
	}
	return retList
}

// enforceCheck checks if the user has permission based on the Casbin model and policy.
// It returns true if the user is authorized, otherwise false.
func enforceCheck(enforcer *casbin.Enforcer, user, resource, object, action string) bool {
	ok, _ := enforcer.Enforce(user, resource, object, action)
	return ok
}

// ConfigFileReload is used to reload the config file when it is changed. This is used to reload the policy without
// restarting the server. The config file is in the format of yaml. The config file is read by viper.
func ConfigFileReload(e fsnotify.Event) {
	logger.Infow("RBAC conf file updated:", "fileName", e.Name)
	err := configReader.ReadInConfig()
	if err != nil {
		return
	}
	// update the scopes
	newScopes := getRBACScopes(configReader)
	currentScopes = newScopes
	// update the default policy
	policyDefault = getDefaultPolicy(configReader)
	// clear the userPermCount cache
	userPermCount = make(map[string]int)
	logger.Infow("Auth Scopes Updated", "scopes", currentScopes)
}

// getDefaultPolicy returns the default policy from the rbac properties file. The default policy is used when the
// requested resource is not present in the policy.
// The default policy is provided in the rbac properties file in the format "policy.default: value"
// Example: policy.default: deny
func getDefaultPolicy(config *viper.Viper) string {
	defaultPolicy := getRbacProperty(RbacPropertyDefaultPolicy, config)
	if defaultPolicy == emptyString {
		return emptyString
	}
	return defaultPolicy.(string)
}

// getPermissionCount returns if there are permissions defined for a user in the RBAC policy.
// If the user has permissions in the policy, it returns true, otherwise false.
func getPermissionCount(enforcer *casbin.Enforcer, user string) bool {
	// check if user exists in userPermCount
	if userPermCount == nil {
		userPermCount = make(map[string]int)
	}
	val, ok := userPermCount[user]
	// If the key exists
	if ok {
		// Return true if the user has permissions in the policy
		// and false if the user does not have permissions in the policy.
		return val > 0
	}
	// get the permissions for the user
	cnt, err := enforcer.GetImplicitPermissionsForUser(user)
	if err != nil {
		logger.Errorw("Failed to get permissions for user", "user", user, "error", err)
		return false
	}
	count := len(cnt)
	// store the count in userPermCount
	userPermCount[user] = count
	return count > 0
}
