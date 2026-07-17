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

package v2

import (
	"github.com/gin-gonic/gin"
)

type handler struct {
	sysInfo any
}

// NewHandler creates a v2 API handler.
func NewHandler(sysInfo any) *handler {
	return &handler{sysInfo: sysInfo}
}

// SysInfo returns system information for the UI / clients.
func (h *handler) SysInfo(c *gin.Context) {
	WriteOK(c, h.sysInfo)
}
