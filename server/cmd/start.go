package cmd

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	"github.com/numaproj/numaflow/server/routes"
)

var (
	rewritePathPrefixes = []string{
		"/namespaces",
	}
)

func Start(insecure bool, port int, namespaced bool, managedNamespace string) {
	logger := logging.NewLogger().Named("server")
	router := gin.New()
	router.Use(gin.Logger())
	router.RedirectTrailingSlash = true
	router.Use(static.Serve("/", static.LocalFile("./ui/build", true)))
	if namespaced {
		router.Use(Namespace(managedNamespace))
	}
	routes.Routes(router)
	router.Use(UrlRewrite(router))
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}

	if insecure {
		logger.Infof("Starting server (TLS disabled) on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil {
			panic(err)
		}
	} else {
		cert, err := sharedtls.GenerateX509KeyPair()
		if err != nil {
			panic(err)
		}
		server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{*cert}, MinVersion: tls.VersionTLS12}

		logger.Infof("Starting server on %s", server.Addr)
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

func Namespace(ns string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("namespace", ns)
		c.Next()
	}
}
