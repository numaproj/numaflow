package authz

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/server/authn"
)

const (
	testPolicyMapPath    = "testData/test-policy-map.csv"
	testPropertyFilePath = "testData/test-rbac-conf.yaml"
	testUrlPath          = "/test"
	testEmail            = "test@test.com"
)

var (
	groupReadOnly = []string{"role:readonly"}
	groupAdmin    = []string{"role:admin"}
	groupDefault  = []string{"role:dev"}
)

// TestCreateAuthorizer is a test implementation of the NewCasbinObject function.
func TestCreateAuthorizer(t *testing.T) {
	authorizer, err := NewCasbinObject(WithPolicyMap(testPolicyMapPath), WithPropertyFile(testPropertyFilePath))
	assert.NoError(t, err)

	// Test that the authorizer is not nil
	assert.NotNil(t, authorizer)

	// Check the roles loaded from the policy map
	roles := authorizer.enforcer.GetAllRoles()
	assert.Equal(t, 2, len(roles))
}

// TestAuthorize is a test implementation of the Authorize functionality.
// It tests that the user is authorized correctly for the given request.
func TestAuthorize(t *testing.T) {
	authorizer, err := NewCasbinObject(WithPolicyMap(testPolicyMapPath), WithPropertyFile(testPropertyFilePath))
	assert.NoError(t, err)

	// Test that the authorizer is not nil
	assert.NotNil(t, authorizer)

	// get a test gin context for GET request
	getRequest := mockHttpRequest("GET")

	// Test that the user is authorized for the GET request
	readOnlyUser := mockUserInfo(testEmail, groupReadOnly)
	isAuth := authorizer.Authorize(getRequest, readOnlyUser)
	assert.True(t, isAuth)

	// Test that the admin user is authorized for the GET request
	adminUser := mockUserInfo(testEmail, groupAdmin)
	isAuth = authorizer.Authorize(getRequest, adminUser)
	assert.True(t, isAuth)

	postRequest := mockHttpRequest("POST")

	// Test that the ReadOnly user is not
	// authorized for the POST request
	isAuth = authorizer.Authorize(postRequest, readOnlyUser)
	assert.False(t, isAuth)

	// Test that the admin user is authorized for the POST request
	isAuth = authorizer.Authorize(postRequest, adminUser)
	assert.True(t, isAuth)
}

// TestDefaultPolicy is a test implementation of the DefaultPolicy functionality.
// It tests that the default policy is set correctly when the authorizer is created.
// Additionally, it tests that the default policy is applied correctly when a user is not found in the policy map.
// The default policy is set to "role:readonly" in the test data.
func TestDefaultPolicy(t *testing.T) {
	authorizer, err := NewCasbinObject(WithPolicyMap(testPolicyMapPath), WithPropertyFile(testPropertyFilePath))
	assert.NoError(t, err)

	// Test that the authorizer is not nil
	assert.NotNil(t, authorizer)

	// Check the default policy, which is set to "role:readonly" in the test data
	defaultPolicy := authorizer.policyDefault
	assert.Equal(t, "role:readonly", defaultPolicy)

	// get a test gin context for GET request
	// The default policy allows only GET requests
	// So, the GET request should be authorized
	getRequest := mockHttpRequest("GET")
	userInfo := mockUserInfo(testEmail, groupDefault)
	isAuth := authorizer.Authorize(getRequest, userInfo)
	assert.True(t, isAuth)

	// get a test gin context for POST request
	// The default policy allows only GET requests
	// So, the POST request should not be authorized
	postRequest := mockHttpRequest("POST")
	isAuth = authorizer.Authorize(postRequest, userInfo)
	assert.False(t, isAuth)
}

// TestScopes is a test implementation of the Scopes functionality.
// It tests that the scopes are set correctly when the authorizer is created.
// Additionally, it tests that the required scopes are tested for the user.
func TestScopes(t *testing.T) {
	authorizer, err := NewCasbinObject(WithPolicyMap(testPolicyMapPath), WithPropertyFile(testPropertyFilePath))
	assert.NoError(t, err)

	// Test that the authorizer is not nil
	assert.NotNil(t, authorizer)

	// Check the scopes loaded from the policy map
	scopes := authorizer.currentScopes
	assert.Equal(t, 1, len(scopes))
	assert.Equal(t, "groups", scopes[0])

	userInfo := mockUserInfo(testEmail, groupReadOnly)

	// get a test gin context for GET request
	// The default policy allows only GET requests
	// So, the GET request should be authorized
	getRequest := mockHttpRequest("GET")
	isAuth := authorizer.Authorize(getRequest, userInfo)
	assert.True(t, isAuth)

	// Change the scopes to "email"
	authorizer.currentScopes = []string{"email"}
	scopes = authorizer.currentScopes
	assert.Equal(t, 1, len(scopes))
	assert.Equal(t, "email", scopes[0])

	// GET requests are not allowed for the test user
	isAuth = authorizer.Authorize(getRequest, userInfo)
	assert.False(t, isAuth)

	// get a test gin context for POST request
	// POST requests are allowed for the test user email
	postRequest := mockHttpRequest("POST")
	isAuth = authorizer.Authorize(postRequest, userInfo)
	assert.True(t, isAuth)
}

// TestConfigFileReload is a test implementation of the ConfigFileReload functionality.
// It tests that the RBAC properties are reloaded correctly when the config file is changed.
func TestConfigFileReload(t *testing.T) {
	authorizer, err := NewCasbinObject(WithPolicyMap(testPolicyMapPath), WithPropertyFile(testPropertyFilePath))
	assert.NoError(t, err)

	// Test that the authorizer is not nil
	assert.NotNil(t, authorizer)

	// Check the RBAC properties loaded from the config file
	defaultPolicy := authorizer.policyDefault
	assert.Equal(t, "role:readonly", defaultPolicy)

	// Change the config file path to test data
	authorizer.configReader.Set(RbacPropertyScopes, ScopeEmail)
	err = authorizer.configReader.WriteConfig()
	if err != nil {
		return
	}

	// Reload the RBAC properties
	scopes := authorizer.currentScopes
	assert.Equal(t, "email", scopes[0])

	// Change the config file path to test data
	authorizer.configReader.Set(RbacPropertyScopes, ScopeGroup)
	err = authorizer.configReader.WriteConfig()
	if err != nil {
		return
	}

	// Reload the RBAC properties
	scopes = authorizer.currentScopes
	assert.Equal(t, "groups", scopes[0])
}

// createTestGinContext is a helper function to create a test gin context.
func createTestGinContext() *gin.Context {
	// Create a new HTTP response recorder
	w := httptest.NewRecorder()

	// Create a new Gin context with the request and response recorder
	c, _ := gin.CreateTestContext(w)
	return c
}

// mockHttpRequest is a helper function to create a mock HTTP request.
// It takes a method type and a user as input.
// It returns a struct of type gin.Context
func mockHttpRequest(method string) *gin.Context {
	req, _ := http.NewRequest(method, testUrlPath, nil)
	c := createTestGinContext()
	c.Request = req
	return c
}

// mockUserInfo is a helper function to mock the user info. It returns a struct of type UserInfo
func mockUserInfo(email string, groups []string) *authn.UserInfo {
	idClaims := mockIdClaims(groups, email)
	return &authn.UserInfo{
		IDTokenClaims: idClaims,
	}
}

// mockIdClaims is a helper function to mock the IDTokenClaims. It returns a struct of type IDTokenClaims
func mockIdClaims(groups []string, email string) *authn.IDTokenClaims {
	return &authn.IDTokenClaims{
		Email:  email,
		Groups: groups,
	}
}
