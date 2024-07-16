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

package grpc_resolver

import (
	"log"
	"strconv"
	"strings"

	"google.golang.org/grpc/resolver"

	"github.com/numaproj/numaflow/pkg/sdkclient"
)

const (
	CustScheme      = "udf"
	CustServiceName = "numaflow.numaproj.io"
	ConnAddr        = "unix://"
)

type MultiProcResolverBuilder struct {
	addrsList []string
}

func newMultiProcResolverBuilder(addrsList []string) *MultiProcResolverBuilder {
	return &MultiProcResolverBuilder{
		addrsList: addrsList,
	}
}

func (m *MultiProcResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &multiProcResolver{
		target: target,
		cc:     cc,
		addrsStore: map[string][]string{
			CustServiceName: m.addrsList,
		},
	}
	r.start()
	return r, nil
}
func (*MultiProcResolverBuilder) Scheme() string { return CustScheme }

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

// buildConnAddrs Populate the connection list for the clients
// Format (serverAddr, serverIdx) :
// (unix:///var/run/numaflow/multiproc0.sock, 0),
// (unix:///var/run/numaflow/multiproc1.sock, 1)
func buildConnAddrs(numServers int) []string {
	var conn = make([]string, numServers)
	for i := 0; i < numServers; i++ {
		// Create a server port assign to each client
		// unix:///var/run/numaflow/multiproc#serv_num.sock
		serverAddr := ConnAddr + sdkclient.MultiProcAddr + strconv.Itoa(i) + ".sock"
		// Format (serverAddr, serverIdx)
		conn[i] = serverAddr + "," + strconv.Itoa(i)
	}
	return conn
}

// RegMultiProcResolver  is used to populate the connection properties based
// on multiprocessing TCP or UDS connection
func RegMultiProcResolver(numServers int) error {
	log.Println("Multiprocessing Servers :", numServers)
	conn := buildConnAddrs(numServers)
	res := newMultiProcResolverBuilder(conn)
	resolver.Register(res)
	return nil
}
