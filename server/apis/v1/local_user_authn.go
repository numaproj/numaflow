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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/common"
)

type LocalUsersAuthObject struct {
	kubeClient   kubernetes.Interface
	authDisabled bool
	log          *zap.SugaredLogger
}

// NewLocalUsersAuthObject is used to provide a new LocalUsersAuthObject
func NewLocalUsersAuthObject(ctx context.Context, authDisabled bool) (*LocalUsersAuthObject, error) {
	var (
		k8sRestConfig *rest.Config
		err           error
	)
	k8sRestConfig, err = util.K8sRestConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeRestConfig, %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(k8sRestConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeclient, %w", err)
	}

	return &LocalUsersAuthObject{
		kubeClient:   kubeClient,
		authDisabled: authDisabled,
		log:          logging.FromContext(ctx),
	}, nil
}

func (l *LocalUsersAuthObject) Authenticate(c *gin.Context) (*authn.UserInfo, error) {
	tokenString, err := c.Cookie("jwt")
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve user identity token: %v", err)
	}
	if tokenString == "" {
		return nil, fmt.Errorf("failed to retrieve user identity token: empty token")
	}

	claims, err := l.ParseToken(c, tokenString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse user identity token: %v", err)
	}

	itc := authn.IDTokenClaims{
		Iss:  claims["iss"].(string),
		Exp:  int(claims["exp"].(float64)),
		Iat:  int(claims["iat"].(float64)),
		Name: claims["username"].(string),
	}
	userInfo := authn.NewUserInfo(&itc, tokenString, "")
	return &userInfo, nil
}

func (l *LocalUsersAuthObject) getSecretKey(ctx context.Context) ([]byte, error) {
	var namespace = util.LookupEnvStringOr("NAMESPACE", "numaflow-system")

	secret, err := l.kubeClient.CoreV1().Secrets(namespace).Get(ctx, common.NumaflowAccountsSecret, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	secretKey, ok := secret.Data[common.NumaflowServerSecretKey]
	if !ok {
		return nil, fmt.Errorf("%s not found in secret", common.NumaflowServerSecretKey)
	}
	return secretKey, nil
}

func (l *LocalUsersAuthObject) VerifyUser(c *gin.Context, username string, password string) error {
	if username == "" || password == "" {
		return fmt.Errorf("username or password cannot be empty")
	}

	ctx := logging.WithLogger(c.Request.Context(), l.log)
	account, err := authn.GetAccount(ctx, username, l.kubeClient)
	if err != nil {
		return err
	}

	if !account.Enabled {
		return fmt.Errorf("account: %s is disabled", username)
	}

	if account.PasswordHash != "" {
		if err = authn.VerifyPassword(password, account.PasswordHash); err != nil {
			return fmt.Errorf("incorrect password enter for the user")
		}
	} else if username == common.NumaflowAdminUsername && account.InitialPasswordHash != "" {
		if err = authn.VerifyInitialPassword(password, account.InitialPasswordHash); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("password not configured for the user")
	}

	return nil
}

// GenerateToken generates a jwt token for the given username
func (l *LocalUsersAuthObject) GenerateToken(c *gin.Context, username string) (string, error) {
	claims := jwt.MapClaims{
		"username": username,
		"exp":      time.Now().Add(time.Hour * 9).Unix(), // Token expiration time - 9hours
		"iat":      time.Now().Unix(),                    // Token issued at time
		"iss":      common.TokenIssuer,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	secretKey, err := l.getSecretKey(c.Request.Context())
	if err != nil {
		return "", err
	}
	signedToken, err := token.SignedString(secretKey)
	if err != nil {
		return "", err
	}
	return signedToken, nil
}

// ParseToken parses a jwt token and returns the claims
func (l *LocalUsersAuthObject) ParseToken(c *gin.Context, tokenString string) (jwt.MapClaims, error) {
	secretKey, err := l.getSecretKey(c.Request.Context())
	if err != nil {
		return nil, err
	}
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, jwt.ErrSignatureInvalid
		}
		return secretKey, nil
	})
	if err != nil {
		return nil, err
	}
	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		return claims, err
	}
	return nil, err
}
