package v1

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

// CallbackResponse is the response payload for the Callback API.
// TODO - rename to something user identity token related.
type CallbackResponse struct {
	IDTokenClaims IDTokenClaims `json:"id_token_claims"`
	IDToken       string        `json:"id_token"`
	RefreshToken  string        `json:"refresh_token"`
}

// NewCallbackResponse return a Callback Response Object.
func NewCallbackResponse(itc IDTokenClaims, idToken string, refreshToken string) CallbackResponse {
	return CallbackResponse{
		IDTokenClaims: itc,
		IDToken:       idToken,
		RefreshToken:  refreshToken,
	}
}
