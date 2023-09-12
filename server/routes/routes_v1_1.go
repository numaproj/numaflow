package routes

import (
	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/server/apis/v1_1"
)

func v1_1Routes(r gin.IRouter) {
	handler, err := v1_1.NewHandler()
	if err != nil {
		panic(err)
	}
	r.GET("/namespaces/:namespace/pipelines", handler.ListPipelines)
}
