package authz

const (
	// PolicyMapPath is the path to the policy map.
	policyMapPath = "/etc/numaflow/rbac-policy.csv"

	// Objects for the RBAC policy
	ObjectAll      = "*"
	ObjectPipeline = "pipeline"
	ObjectISBSvc   = "isbsvc"
	ObjectEvents   = "events"

	// Resouces for the RBAC policy
	ResourceAll       = "*"
	ResourceNamespace = "namespace"

	// MatchAll is a wildcard to match all patterns
	MatchAll = "*"
)
