package poddiscovery

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
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
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{Name: "my-mv", Namespace: "test"},
	}
	pl := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pipeline", Namespace: "test"},
	}
	assert.Equal(t, ResolveRequest{
		HeadlessService: "my-mv-mv-headless",
		Namespace:       "test",
	}, MonoVertexRequest(mv))
	assert.Equal(t, ResolveRequest{
		HeadlessService: "my-pipeline-my-vertex-headless",
		Namespace:       "test",
	}, PipelineVertexRequest(pl, "my-vertex"))
}

func TestDNSResolverResolve(t *testing.T) {
	lookup := &fakeHostLookup{ips: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}}
	resolver := &dnsResolver{lookup: lookup, timeout: time.Second}
	pl := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: "default"},
	}
	req := PipelineVertexRequest(pl, "vertex")

	count, err := resolver.Resolve(context.Background(), req)

	require.NoError(t, err)
	assert.Equal(t, 3, count)
	assert.Equal(t, "pipeline-vertex-headless.default.svc", lookup.host)
}

func TestDNSResolverResolveNoPods(t *testing.T) {
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{Name: "mono", Namespace: "default"},
	}
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

			count, err := resolver.Resolve(context.Background(), MonoVertexRequest(mv))

			require.NoError(t, err)
			assert.Zero(t, count)
		})
	}
}

func TestDNSResolverResolveError(t *testing.T) {
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{Name: "mono", Namespace: "default"},
	}
	resolver := &dnsResolver{
		lookup:  &fakeHostLookup{err: &net.DNSError{Err: "server misbehaving", IsTemporary: true}},
		timeout: time.Second,
	}

	count, err := resolver.Resolve(context.Background(), MonoVertexRequest(mv))

	assert.Error(t, err)
	assert.Zero(t, count)
}

func TestDNSResolverResolveTimeout(t *testing.T) {
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{Name: "mono", Namespace: "default"},
	}
	resolver := &dnsResolver{
		lookup:  &fakeHostLookup{wait: true},
		timeout: time.Millisecond,
	}

	count, err := resolver.Resolve(context.Background(), MonoVertexRequest(mv))

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Zero(t, count)
}
