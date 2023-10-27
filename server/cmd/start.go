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
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	v1 "github.com/numaproj/numaflow/server/apis/v1"
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
	Insecure         bool
	Port             int
	Namespaced       bool
	ManagedNamespace string
	BaseHref         string
	DisableAuth      bool
	DexServerAddr    string
	DexProxyAddr     string
	ServerAddr       string
}

type server struct {
	options ServerOptions
}

func NewServer(opts ServerOptions) *server {
	return &server{
		options: opts,
	}
}

func (s *server) Start() {
	logger := logging.NewLogger().Named("server")
	router := gin.New()
	router.Use(gin.LoggerWithConfig(gin.LoggerConfig{SkipPaths: []string{"/livez"}}))
	router.RedirectTrailingSlash = true
	router.Use(static.Serve(s.options.BaseHref, static.LocalFile("./ui/build", true)))
	if s.options.BaseHref != "/" {
		router.NoRoute(func(c *gin.Context) {
			c.File("./ui/build/index.html")
		})
	}
	router.Any("/dex/*name", v1.NewDexReverseProxy(s.options.DexServerAddr))
	routes.Routes(
		router,
		routes.SystemInfo{
			ManagedNamespace: s.options.ManagedNamespace,
			Namespaced:       s.options.Namespaced,
			Version:          numaflow.GetVersion().String()},
		routes.AuthInfo{
			DisableAuth:   s.options.DisableAuth,
			DexServerAddr: s.options.DexServerAddr,
			DexProxyAddr:  s.options.DexProxyAddr,
			ServerAddr:    s.options.ServerAddr,
		},
		s.options.BaseHref,
	)
	router.Use(UrlRewrite(router))
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", s.options.Port),
		Handler: router,
	}

	if s.options.Insecure {
		logger.Infow(
			"Starting server (TLS disabled) on "+server.Addr,
			"version", numaflow.GetVersion(),
			"disable-auth", s.options.DisableAuth,
			"dex-server-addr", s.options.DexServerAddr,
			"dex-proxy-addr", s.options.DexProxyAddr,
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
		logger.Infow(
			"Starting server on "+server.Addr,
			"version", numaflow.GetVersion(),
			"disable-auth", s.options.DisableAuth,
			"dex-server-addr", s.options.DexServerAddr,
			"dex-proxy-addr", s.options.DexProxyAddr,
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
