package casbin

type AuthorizationRequest struct {
	Groups   []string
	Resource string
	Action   string
	// TODO - add more fields?
}

func IsAuthorized(r AuthorizationRequest) bool {
	// TODO - implement
	return true
}
