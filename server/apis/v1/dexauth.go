package v1

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/gin-gonic/gin"
	"golang.org/x/oauth2"
)

// DexObject is a struct that holds details for dex handlers.
type DexObject struct {
	clientID    string
	redirectURI string

	verifier *oidc.IDTokenVerifier
	provider *oidc.Provider

	// offlineAsScope defines whether the provider uses "offline_access" scope to
	// request a refresh token or uses "access_type=offline" (e.g. Google)
	offlineAsScope bool

	client     *http.Client
	stateNonce string // stateNonce is the nonce variable
}

func (d *DexObject) oauth2Config(scopes []string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:    d.clientID,
		Endpoint:    d.provider.Endpoint(),
		Scopes:      scopes,
		RedirectURL: d.redirectURI,
	}
}

// NewDexObject returns a new DexObject.
// TODO: refactor data structure and make configurable
func NewDexObject(ctx context.Context, baseURL string, proxyURL string) *DexObject {
	// TODO: make const
	clientID := "numaflow-server-app"
	issuerURL, err := url.JoinPath(baseURL, "/dex")
	_ = err
	redirectURI, err := url.JoinPath(baseURL, "/login")
	_ = err
	client := http.DefaultClient
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client.Transport = NewDexRewriteURLRoundTripper(proxyURL, client.Transport)
	newCtx := oidc.ClientContext(ctx, client)
	provider, err := oidc.NewProvider(newCtx, issuerURL)
	if err != nil {
		log.Fatalf("failed to query provider %q: %v", issuerURL, err)
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: clientID})

	return &DexObject{
		clientID:       clientID,
		redirectURI:    redirectURI,
		verifier:       verifier,
		provider:       provider,
		offlineAsScope: true,
		client:         client,
	}
}

func (d *DexObject) handleLogin(c *gin.Context) {
	var scopes []string
	authCodeURL := ""
	scopes = append(scopes, "openid", "profile", "email", "groups")
	// stateNonce is an OAuth2 state nonce
	d.stateNonce = generateRandomNumber(10)
	if d.offlineAsScope {
		scopes = append(scopes, "offline_access")
		authCodeURL = d.oauth2Config(scopes).AuthCodeURL(d.stateNonce)
	} else {
		authCodeURL = d.oauth2Config(scopes).AuthCodeURL(d.stateNonce, oauth2.AccessTypeOffline)
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, NewLoginResponse(authCodeURL)))
}

func (d *DexObject) handleCallback(c *gin.Context) {
	var (
		r     = c.Request
		err   error
		token *oauth2.Token
	)
	ctx := oidc.ClientContext(r.Context(), d.client)
	oauth2Config := d.oauth2Config(nil)

	// Authorization redirect callback from OAuth2 auth flow.
	if errMsg := r.FormValue("error"); errMsg != "" {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	code := r.FormValue("code")
	if code == "" {
		errMsg := "Missing code in the request."
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	// TODO: currently looks like it only works when only one user does the login
	// if state := r.FormValue("state"); state != d.stateNonce {
	// 	errMsg := fmt.Sprintf("Expected state %q got %q", d.stateNonce, state)
	// 	c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
	// 	return
	// }

	token, err = oauth2Config.Exchange(ctx, code)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get token: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		errMsg := fmt.Sprintf("Failed to get id_token: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	idToken, err := d.verifier.Verify(r.Context(), rawIDToken)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to verify ID token: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	var claims IDTokenClaims
	if err := idToken.Claims(&claims); err != nil {
		errMsg := fmt.Sprintf("error decoding ID token claims: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	var refreshToken string
	if refreshToken, ok = token.Extra("refresh_token").(string); !ok {
		errMsg := "Failed to convert refresh_token"
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	res := NewCallbackResponse(claims, rawIDToken, refreshToken)
	tokenStr, err := json.Marshal(res)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to convert to token string: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.SetCookie("user-identity-token", string(tokenStr), 3600, "/", "", true, true)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, res))
}

// generateRandomNumber is for generating state nonce. This piece of code was obtained without much change from the argo-cd repository.
// from a given charset generates a cryptographically-secure pseudo-random string of a given length.
func generateRandomNumber(n int) string {
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	src := rand.NewSource(time.Now().UnixNano())
	letterIdxBits := 6                    // 6 bits to represent a letter index
	letterIdxMask := 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax := 63 / letterIdxBits    // # of letter indices fitting in 63 bits
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache) & letterIdxMask; idx < len(charset) {
			b[i] = charset[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

// NewDexReverseProxy sends the dex request to the dex server.
func NewDexReverseProxy(target string) func(c *gin.Context) {
	return func(c *gin.Context) {
		proxyUrl, _ := url.Parse(target)
		c.Request.URL.Path = c.Param("name")
		proxy := httputil.NewSingleHostReverseProxy(proxyUrl)
		fmt.Println("proxy", proxyUrl, c.Request.URL.Path)
		proxy.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

// NewDexRewriteURLRoundTripper creates a new DexRewriteURLRoundTripper
func NewDexRewriteURLRoundTripper(dexServerAddr string, T http.RoundTripper) DexRewriteURLRoundTripper {
	dexURL, _ := url.Parse(dexServerAddr)
	return DexRewriteURLRoundTripper{
		DexURL: dexURL,
		T:      T,
	}
}

// DexRewriteURLRoundTripper is an HTTP RoundTripper to rewrite HTTP requests to the specified
// dex server address. This is used when reverse proxying Dex to avoid the API server from
// unnecessarily communicating to the numaflow server through its externally facing load balancer, which is not
// always permitted in firewalled/air-gapped networks.
type DexRewriteURLRoundTripper struct {
	DexURL *url.URL
	T      http.RoundTripper
}

func (s DexRewriteURLRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Host = s.DexURL.Host
	r.URL.Scheme = s.DexURL.Scheme
	return s.T.RoundTrip(r)
}
