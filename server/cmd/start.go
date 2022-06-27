package cmd

import (
	"crypto/tls"
	"net/http"
	"strings"

	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"

	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	"github.com/numaproj/numaflow/server/routes"
)

var (
	rewritePathPrefixes = []string{
		"/namespaces",
	}
)

func Start() {
	router := gin.New()
	router.Use(gin.Logger())
	router.RedirectTrailingSlash = true
	router.Use(static.Serve("/", static.LocalFile("./ui/build", true)))
	routes.Routes(router)
	router.Use(UrlRewrite(router))
	cert, err := sharedtls.GenerateX509KeyPair()
	if err != nil {
		panic(err)
	}
	server := http.Server{
		Addr:      ":8443",
		Handler:   router,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{*cert}, MinVersion: tls.VersionTLS12},
	}
	if err := server.ListenAndServeTLS("", ""); err != nil {
		panic(err)
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
