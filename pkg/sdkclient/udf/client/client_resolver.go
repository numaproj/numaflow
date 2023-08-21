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

package client

import (
	"strconv"
	"strings"

	"github.com/numaproj/numaflow-go/pkg/function"
	"google.golang.org/grpc/resolver"
)

const (
	custScheme      = "udf"
	custServiceName = "numaflow.numaproj.io"
	connAddr        = "0.0.0.0"
)

type multiProcResolverBuilder struct {
	addrsList []string
}

func (m *multiProcResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &multiProcResolver{
		target: target,
		cc:     cc,
		addrsStore: map[string][]string{
			custServiceName: m.addrsList,
		},
	}
	r.start()
	return r, nil
}
func (*multiProcResolverBuilder) Scheme() string { return custScheme }

type multiProcResolver struct {
	target     resolver.Target
	cc         resolver.ClientConn
	addrsStore map[string][]string
}

func (r *multiProcResolver) start() {
	addrStrs := r.addrsStore[r.target.Endpoint()]
	addrs := make([]resolver.Address, len(addrStrs))
	//attrs := make(*attributes.Attributes, len(addrStrs))
	for i, s := range addrStrs {
		adr := strings.Split(s, ",")
		addr := adr[0]
		serv := adr[1]
		addrs[i] = resolver.Address{Addr: addr, ServerName: serv}
	}
	err := r.cc.UpdateState(resolver.State{Addresses: addrs})
	if err != nil {
		return
	}
}
func (*multiProcResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (*multiProcResolver) Close()                                  {}
func (*multiProcResolver) Resolve(target resolver.Target)          {}

// Populate the connection list for the clients
// Format (serverAddr, serverIdx) : (0.0.0.0:5551, 1)
func buildConnAddrs(numCpu int) []string {
	var conn = make([]string, numCpu)
	for i := 0; i < numCpu; i++ {
		conn[i] = connAddr + function.TcpAddr + "," + strconv.Itoa(i+1)
	}
	return conn
}
