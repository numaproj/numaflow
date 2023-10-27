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

package v1

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/gin-gonic/gin"
	"golang.org/x/oauth2"

	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/common"
)

// DexObject is a struct that holds details for dex handlers.
type DexObject struct {
	clientID    string
	issuerURL   string
	redirectURI string
	// offlineAsScope defines whether the provider uses "offline_access" scope to
	// request a refresh token or uses "access_type=offline" (e.g. Google)
	offlineAsScope bool
	client         *http.Client
}

// NewDexObject returns a new DexObject.
func NewDexObject(baseURL string, baseHref string, proxyURL string) (*DexObject, error) {
	issuerURL, err := url.JoinPath(baseURL, "/dex")
	if err != nil {
		return nil, err
	}
	redirectURI, err := url.JoinPath(baseURL, baseHref, "/login")
	if err != nil {
		return nil, err
	}
	client := http.DefaultClient
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client.Transport = NewDexRewriteURLRoundTripper(proxyURL, client.Transport)
	return &DexObject{
		clientID:       common.AppClientID,
		issuerURL:      issuerURL,
		redirectURI:    redirectURI,
		offlineAsScope: true,
		client:         client,
	}, nil
}

func (d *DexObject) provider(client *http.Client, issuerURL string) (*oidc.Provider, error) {
	newCtx := oidc.ClientContext(context.Background(), client)
	return oidc.NewProvider(newCtx, issuerURL)
}

func (d *DexObject) verifier() (*oidc.IDTokenVerifier, error) {
	provider, err := d.provider(d.client, d.issuerURL)
	if err != nil {
		return nil, err
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: common.AppClientID})
	return verifier, nil
}

func (d *DexObject) oauth2Config(scopes []string) (*oauth2.Config, error) {
	provider, err := d.provider(d.client, d.issuerURL)
	if err != nil {
		return nil, err
	}
	return &oauth2.Config{
		ClientID:    d.clientID,
		Endpoint:    provider.Endpoint(),
		Scopes:      scopes,
		RedirectURL: d.redirectURI,
	}, nil
}

func (d *DexObject) Authenticate(c *gin.Context) (*authn.UserInfo, error) {
	var userInfo authn.UserInfo
	userIdentityTokenStr, err := c.Cookie(common.UserIdentityCookieName)
	if err != nil {
		return nil, fmt.Errorf("failed to get user identity token from cookie: %v", err)
	}
	if err = json.Unmarshal([]byte(userIdentityTokenStr), &userInfo); err != nil {
		return nil, fmt.Errorf("failed to parse user identity token: %v", err)
	}
	_, err = d.verify(c, userInfo.IDToken)
	if err != nil {
		return nil, err
	}
	return &userInfo, nil
}

// verify is used to validate the user ID token.
func (d *DexObject) verify(ctx context.Context, rawIDToken string) (*oidc.IDToken, error) {
	verifier, err := d.verifier()
	if err != nil {
		return nil, err
	}
	// the oidc library will verify the token for us:
	// validate the id token
	// check malformed jwt token
	// check issuer
	// check audience
	// check expiry
	// check signature
	return verifier.Verify(ctx, rawIDToken)
}

func (d *DexObject) handleLogin(c *gin.Context) {
	var scopes []string
	authCodeURL := ""
	scopes = append(scopes, "openid", "profile", "email", "groups")
	// stateNonce is an OAuth2 state nonce
	stateNonce := generateRandomNumber(10)
	if d.offlineAsScope {
		scopes = append(scopes, "offline_access")
		oauth2Config, err := d.oauth2Config(scopes)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get oauth2 config %v", err)
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		}
		authCodeURL = oauth2Config.AuthCodeURL(stateNonce)
	} else {
		oauth2Config, err := d.oauth2Config(scopes)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get oauth2 config %v", err)
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		}
		authCodeURL = oauth2Config.AuthCodeURL(stateNonce, oauth2.AccessTypeOffline)
	}
	cookieValue := hex.EncodeToString([]byte(stateNonce))
	c.SetCookie(common.StateCookieName, cookieValue, common.StateCookieMaxAge, "/", "", true, true)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, NewLoginResponse(authCodeURL)))
}

func (d *DexObject) handleCallback(c *gin.Context) {
	var (
		r     = c.Request
		err   error
		token *oauth2.Token
	)
	ctx := oidc.ClientContext(r.Context(), d.client)
	oauth2Config, err := d.oauth2Config(nil)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get oauth2 config %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
	}
	stateCookie, err := c.Cookie(common.StateCookieName)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get state cookie: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	val, err := hex.DecodeString(stateCookie)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to decode state: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	if state := r.FormValue("state"); state != string(val) {
		errMsg := fmt.Sprintf("Expected state %q got %q", string(val), state)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	// delete after read, we only need it for login flow
	c.SetCookie(common.StateCookieName, "", -1, "/", "", true, true)

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

	idToken, err := d.verify(r.Context(), rawIDToken)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to verify ID token: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	var claims authn.IDTokenClaims
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

	res := authn.NewUserInfo(claims, rawIDToken, refreshToken)
	tokenStr, err := json.Marshal(res)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to convert to token string: %v", err)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.SetCookie(common.UserIdentityCookieName, string(tokenStr), common.UserIdentityCookieMaxAge, "/", "", true, true)
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
