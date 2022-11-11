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

package main

import (
	"fmt"
	"net/url"
	"strconv"

	"k8s.io/apimachinery/pkg/util/rand"
)

type messageFactory struct {
	prefix string
	size   int
}

func newMessageFactory(v url.Values) messageFactory {
	prefix := v.Get("prefix")
	if prefix == "" {
		prefix = "random-"
	}
	size, _ := strconv.Atoi(v.Get("size"))
	return messageFactory{prefix: prefix, size: size}
}

func (f messageFactory) newMessage(i int) string {
	y := fmt.Sprintf("%s-%d", f.prefix, i)
	if f.size > 0 {
		y += "-"
		y += rand.String(f.size)
	}
	return y
}
