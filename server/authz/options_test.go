package authz

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWithPolicyMap is a test implementation of the WithPolicyMap function.
// It tests that the policy map path is set correctly.
func TestWithPolicyMap(t *testing.T) {
	var (
		testPolicyMapPath = "test-policy-map-path"
		opts              = &options{
			policyMapPath: policyMapPath,
		}
	)
	WithPolicyMap(testPolicyMapPath)(opts)
	assert.Equal(t, testPolicyMapPath, opts.policyMapPath)
}

// TestWithPropertyFile is a test implementation of the WithPropertyFile function.
// It tests that the property file path is set correctly.
func TestWithPropertyFile(t *testing.T) {
	var (
		testPropertyFilePath = "test-property-file-path"
		opts                 = &options{
			rbacPropertiesPath: rbacPropertiesPath,
		}
	)
	WithPropertyFile(testPropertyFilePath)(opts)
	assert.Equal(t, testPropertyFilePath, opts.rbacPropertiesPath)
}
