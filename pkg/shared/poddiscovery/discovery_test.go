package poddiscovery

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeHostLookup struct {
	host string
	ips  []string
	err  error
	wait bool
}

func (f *fakeHostLookup) LookupHost(ctx context.Context, host string) ([]string, error) {
	f.host = host
	if f.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.ips, f.err
}

func TestResolveRequests(t *testing.T) {
	assert.Equal(t, ResolveRequest{
		HeadlessService: "my-mv-mv-headless",
		Namespace:       "test",
	}, MonoVertexRequest("my-mv", "test"))
	assert.Equal(t, ResolveRequest{
		HeadlessService: "my-pipeline-my-vertex-headless",
		Namespace:       "test",
	}, PipelineVertexRequest("my-pipeline", "my-vertex", "test"))
}

func TestDNSResolverResolve(t *testing.T) {
	lookup := &fakeHostLookup{ips: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}}
	resolver := &dnsResolver{lookup: lookup, timeout: time.Second}
	req := PipelineVertexRequest("pipeline", "vertex", "default")

	indices, err := resolver.Resolve(context.Background(), req)

	require.NoError(t, err)
	assert.Equal(t, []int{0, 1, 2}, indices)
	assert.Equal(t, "pipeline-vertex-headless.default.svc", lookup.host)
}

func TestDNSResolverResolveNoPods(t *testing.T) {
	tests := []struct {
		name string
		ips  []string
		err  error
	}{
		{name: "empty answer"},
		{name: "not found", err: &net.DNSError{IsNotFound: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := &dnsResolver{
				lookup:  &fakeHostLookup{ips: tt.ips, err: tt.err},
				timeout: time.Second,
			}

			indices, err := resolver.Resolve(context.Background(), MonoVertexRequest("mono", "default"))

			require.NoError(t, err)
			assert.Empty(t, indices)
		})
	}
}

func TestDNSResolverResolveError(t *testing.T) {
	resolver := &dnsResolver{
		lookup:  &fakeHostLookup{err: &net.DNSError{Err: "server misbehaving", IsTemporary: true}},
		timeout: time.Second,
	}

	indices, err := resolver.Resolve(context.Background(), MonoVertexRequest("mono", "default"))

	assert.Error(t, err)
	assert.Nil(t, indices)
}

func TestDNSResolverResolveTimeout(t *testing.T) {
	resolver := &dnsResolver{
		lookup:  &fakeHostLookup{wait: true},
		timeout: time.Millisecond,
	}

	indices, err := resolver.Resolve(context.Background(), MonoVertexRequest("mono", "default"))

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, indices)
}
