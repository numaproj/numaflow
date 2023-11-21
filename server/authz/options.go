package authz

type options struct {
	policyMapPath      string
	rbacPropertiesPath string
}

// Option is the interface to apply options.
type Option func(*options)

func DefaultOptions() *options {
	return &options{
		policyMapPath:      policyMapPath,
		rbacPropertiesPath: rbacPropertiesPath,
	}
}

// WithPolicyMap sets the policy map path to be used for the RBAC enforcer
func WithPolicyMap(path string) Option {
	return func(opts *options) {
		opts.policyMapPath = path
	}
}

// WithPropertyFile sets the property file path to be used for the RBAC enforcer
func WithPropertyFile(path string) Option {
	return func(opts *options) {
		opts.rbacPropertiesPath = path
	}

}
