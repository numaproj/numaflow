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
