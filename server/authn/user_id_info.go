package authn

// IDTokenClaims is the claims extract from the IDToken.
type IDTokenClaims struct {
	Iss               string   `json:"iss"`
	Sub               string   `json:"sub"`
	Aud               string   `json:"aud"`
	Exp               int      `json:"exp"`
	Iat               int      `json:"iat"`
	AtHash            string   `json:"at_hash"`
	CHash             string   `json:"c_hash"`
	Email             string   `json:"email"`
	EmailVerified     bool     `json:"email_verified"`
	Groups            []string `json:"groups"`
	Name              string   `json:"name"`
	PreferredUsername string   `json:"preferred_username"`
}

// UserIdInfo includes information about the user identity
// It holds the IDTokenClaims, IDToken and RefreshToken for the user
type UserIdInfo struct {
	IDTokenClaims IDTokenClaims `json:"id_token_claims"`
	IDToken       string        `json:"id_token"`
	RefreshToken  string        `json:"refresh_token"`
}

// NewUserIdInfo return a Callback Response Object.
func NewUserIdInfo(itc IDTokenClaims, idToken string, refreshToken string) UserIdInfo {
	return UserIdInfo{
		IDTokenClaims: itc,
		IDToken:       idToken,
		RefreshToken:  refreshToken,
	}
}
