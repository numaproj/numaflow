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

package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	v1 "github.com/numaproj/numaflow/server/apis/v1"
	"github.com/numaproj/numaflow/server/authz"
	"github.com/numaproj/numaflow/server/routes"
)

var (
	rewritePathPrefixes = []string{
		"/namespaces",
		"/pipelines",
		"/login",
	}
)

type ServerOptions struct {
	Insecure             bool
	Port                 int
	Namespaced           bool
	ManagedNamespace     string
	BaseHref             string
	DisableAuth          bool
	DexServerAddr        string
	ServerAddr           string
	CorsAllowedOrigins   string
	ReadOnly             bool
	DaemonClientProtocol string
}

type server struct {
	options ServerOptions
}

func NewServer(opts ServerOptions) *server {
	return &server{
		options: opts,
	}
}

func (s *server) Start(ctx context.Context) {
	log := logging.FromContext(ctx)
	router := gin.New()
	router.Use(gin.LoggerWithConfig(gin.LoggerConfig{SkipPaths: []string{"/livez"}}))
	allowedOrigins := make([]string, 0)
	if s.options.CorsAllowedOrigins != "" {
		for _, o := range strings.Split(s.options.CorsAllowedOrigins, ",") {
			s := strings.TrimSpace(o)
			s = strings.TrimRight(s, "/") // Remove trailing slash if any
			if len(s) > 0 {
				allowedOrigins = append(allowedOrigins, s)
			}
		}
	}
	if len(allowedOrigins) > 0 {
		router.Use(cors.New(cors.Config{
			AllowOrigins:     allowedOrigins,
			AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"},
			AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type"},
			AllowCredentials: true,
		}))
	}
	router.RedirectTrailingSlash = true
	// sets the route map for authorization with the base href
	authRouteMap := CreateAuthRouteMap(s.options.BaseHref)
	router.Use(static.Serve(s.options.BaseHref, static.LocalFile("./ui/build", true)))
	if s.options.BaseHref != "/" {
		router.NoRoute(func(c *gin.Context) {
			c.File("./ui/build/index.html")
		})
	}
	router.Any("/dex/*name", v1.NewDexReverseProxy(s.options.DexServerAddr))
	routes.Routes(
		ctx,
		router,
		routes.SystemInfo{
			ManagedNamespace:     s.options.ManagedNamespace,
			Namespaced:           s.options.Namespaced,
			IsReadOnly:           s.options.ReadOnly,
			Version:              numaflow.GetVersion().String(),
			DaemonClientProtocol: s.options.DaemonClientProtocol,
		},
		routes.AuthInfo{
			DisableAuth:   s.options.DisableAuth,
			DexServerAddr: s.options.DexServerAddr,
			ServerAddr:    s.options.ServerAddr,
		},
		s.options.BaseHref,
		authRouteMap,
	)
	router.Use(UrlRewrite(router))
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", s.options.Port),
		Handler: router,
	}

	if s.options.Insecure {
		log.Infow(
			"Starting server (TLS disabled) on "+server.Addr,
			"version", numaflow.GetVersion(),
			"disable-auth", s.options.DisableAuth,
			"dex-server-addr", s.options.DexServerAddr,
			"server-addr", s.options.ServerAddr)
		if err := server.ListenAndServe(); err != nil {
			panic(err)
		}
	} else {
		cert, err := sharedtls.GenerateX509KeyPair()
		if err != nil {
			panic(err)
		}
		server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{*cert}, MinVersion: tls.VersionTLS12}
		log.Infow(
			"Starting server on "+server.Addr,
			"version", numaflow.GetVersion(),
			"disable-auth", s.options.DisableAuth,
			"dex-server-addr", s.options.DexServerAddr,
			"server-addr", s.options.ServerAddr)
		if err := server.ListenAndServeTLS("", ""); err != nil {
			panic(err)
		}
	}
}

func needToRewrite(path string) bool {
	for _, p := range rewritePathPrefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

func UrlRewrite(r *gin.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		if needToRewrite(c.Request.URL.Path) {
			c.Request.URL.Path = "/"
			r.HandleContext(c)
		}
		c.Next()
	}
}

// CreateAuthRouteMap creates the route map for authorization.
// The key is a combination of the HTTP method and the path along with the baseHref.
// For example, "GET:/api/v1/namespaces" becomes "GET:/baseHref/api/v1/namespaces".
// The value is a RouteInfo object.
func CreateAuthRouteMap(baseHref string) authz.RouteMap {
	return authz.RouteMap{
		"GET:" + baseHref + "api/v1/sysinfo":                                                         authz.NewRouteInfo(authz.ObjectPipeline, false),
		"GET:" + baseHref + "api/v1/authinfo":                                                        authz.NewRouteInfo(authz.ObjectEvents, false),
		"GET:" + baseHref + "api/v1/namespaces":                                                      authz.NewRouteInfo(authz.ObjectEvents, false),
		"GET:" + baseHref + "api/v1/cluster-summary":                                                 authz.NewRouteInfo(authz.ObjectPipeline, false),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/pipelines":                                 authz.NewRouteInfo(authz.ObjectPipeline, true),
		"POST:" + baseHref + "api/v1/namespaces/:namespace/pipelines":                                authz.NewRouteInfo(authz.ObjectPipeline, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline":                       authz.NewRouteInfo(authz.ObjectPipeline, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline/health":                authz.NewRouteInfo(authz.ObjectPipeline, true),
		"PUT:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline":                       authz.NewRouteInfo(authz.ObjectPipeline, true),
		"DELETE:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline":                    authz.NewRouteInfo(authz.ObjectPipeline, true),
		"PATCH:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline":                     authz.NewRouteInfo(authz.ObjectPipeline, true),
		"POST:" + baseHref + "api/v1/namespaces/:namespace/isb-services":                             authz.NewRouteInfo(authz.ObjectISBSvc, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/isb-services":                              authz.NewRouteInfo(authz.ObjectISBSvc, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/isb-services/:isb-service":                 authz.NewRouteInfo(authz.ObjectISBSvc, true),
		"PUT:" + baseHref + "api/v1/namespaces/:namespace/isb-services/:isb-service":                 authz.NewRouteInfo(authz.ObjectISBSvc, true),
		"DELETE:" + baseHref + "api/v1/namespaces/:namespace/isb-services/:isb-service":              authz.NewRouteInfo(authz.ObjectISBSvc, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline/isbs":                  authz.NewRouteInfo(authz.ObjectPipeline, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline/watermarks":            authz.NewRouteInfo(authz.ObjectPipeline, true),
		"PUT:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex":      authz.NewRouteInfo(authz.ObjectPipeline, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/metrics":      authz.NewRouteInfo(authz.ObjectPipeline, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods": authz.NewRouteInfo(authz.ObjectPipeline, true),
		"GET:" + baseHref + "api/v1/metrics/namespaces/:namespace/pods":                              authz.NewRouteInfo(authz.ObjectPipeline, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/pods/:pod/logs":                            authz.NewRouteInfo(authz.ObjectPipeline, true),
		"GET:" + baseHref + "api/v1/namespaces/:namespace/events":                                    authz.NewRouteInfo(authz.ObjectEvents, true),
	}
}
