package poddiscovery

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSRVLookup struct {
	service string
	proto   string
	name    string
	records []*net.SRV
	err     error
	wait    bool
}

func (f *fakeSRVLookup) LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
	f.service = service
	f.proto = proto
	f.name = name
	if f.wait {
		<-ctx.Done()
		return "", nil, ctx.Err()
	}
	return "", f.records, f.err
}

func TestResolveRequests(t *testing.T) {
	assert.Equal(t, ResolveRequest{
		HeadlessService: "my-mv-mv-headless",
		Namespace:       "test",
		PortName:        "runtime",
		PodNamePrefix:   "my-mv-mv-",
	}, MonoVertexRequest("my-mv", "test", "runtime"))
	assert.Equal(t, ResolveRequest{
		HeadlessService: "my-pipeline-my-vertex-headless",
		Namespace:       "test",
		PortName:        "metrics",
		PodNamePrefix:   "my-pipeline-my-vertex-",
	}, PipelineVertexRequest("my-pipeline", "my-vertex", "test", "metrics"))
}

func TestDNSResolverResolve(t *testing.T) {
	lookup := &fakeSRVLookup{records: []*net.SRV{
		{Target: "pipeline-vertex-2.pipeline-vertex-headless.default.svc."},
		{Target: "pipeline-vertex-0.pipeline-vertex-headless.default.svc."},
		{Target: "pipeline-vertex-2.pipeline-vertex-headless.default.svc."},
		{Target: "unrelated-1.pipeline-vertex-headless.default.svc."},
		{Target: "pipeline-vertex-invalid.pipeline-vertex-headless.default.svc."},
	}}
	resolver := &dnsResolver{lookup: lookup, timeout: time.Second}
	req := PipelineVertexRequest("pipeline", "vertex", "default", "runtime")

	indices, err := resolver.Resolve(context.Background(), req)

	require.NoError(t, err)
	assert.Equal(t, []int{0, 2}, indices)
	assert.Equal(t, "runtime", lookup.service)
	assert.Equal(t, "tcp", lookup.proto)
	assert.Equal(t, "pipeline-vertex-headless.default.svc", lookup.name)
}

func TestDNSResolverResolveNoPods(t *testing.T) {
	tests := []struct {
		name    string
		records []*net.SRV
		err     error
	}{
		{name: "empty answer"},
		{name: "not found", err: &net.DNSError{IsNotFound: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := &dnsResolver{
				lookup:  &fakeSRVLookup{records: tt.records, err: tt.err},
				timeout: time.Second,
			}

			indices, err := resolver.Resolve(context.Background(), MonoVertexRequest("mono", "default", "metrics"))

			require.NoError(t, err)
			assert.Empty(t, indices)
		})
	}
}

func TestDNSResolverResolveError(t *testing.T) {
	tests := []struct {
		name    string
		records []*net.SRV
		err     error
	}{
		{name: "transient DNS error", err: &net.DNSError{Err: "server misbehaving", IsTemporary: true}},
		{name: "network error", err: errors.New("network unavailable")},
		{
			name:    "no matching indexed targets",
			records: []*net.SRV{{Target: "another-pod.service.default.svc."}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := &dnsResolver{
				lookup:  &fakeSRVLookup{records: tt.records, err: tt.err},
				timeout: time.Second,
			}

			indices, err := resolver.Resolve(context.Background(), MonoVertexRequest("mono", "default", "metrics"))

			assert.Error(t, err)
			assert.Nil(t, indices)
		})
	}
}

func TestDNSResolverResolveTimeout(t *testing.T) {
	resolver := &dnsResolver{
		lookup:  &fakeSRVLookup{wait: true},
		timeout: time.Millisecond,
	}

	indices, err := resolver.Resolve(context.Background(), MonoVertexRequest("mono", "default", "metrics"))

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, indices)
}

func TestResolveAndProbePipelineVertex(t *testing.T) {
	resolver := &dnsResolver{
		lookup: &fakeSRVLookup{records: []*net.SRV{
			{Target: "pipeline-vertex-0.pipeline-vertex-headless.default.svc."},
			{Target: "pipeline-vertex-2.pipeline-vertex-headless.default.svc."},
		}},
		timeout: time.Second,
	}
	active, err := ResolveAndProbe(
		context.Background(),
		resolver,
		PipelineVertexRequest("pipeline", "vertex", "default", "metrics"),
		func(index int) bool {
			return index == 2
		},
	)

	require.NoError(t, err)
	assert.Equal(t, []int{2}, active)
}

func TestResolveAndProbeMonoVertex(t *testing.T) {
	resolver := &dnsResolver{
		lookup: &fakeSRVLookup{records: []*net.SRV{
			{Target: "mono-mv-0.mono-mv-headless.default.svc."},
		}},
		timeout: time.Second,
	}
	active, err := ResolveAndProbe(
		context.Background(),
		resolver,
		MonoVertexRequest("mono", "default", "runtime"),
		func(index int) bool {
			return index == 0
		},
	)

	require.NoError(t, err)
	assert.Equal(t, []int{0}, active)
}

func TestResolveAndProbeConcurrentProbing(t *testing.T) {
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	active, err := ResolveAndProbe(context.Background(), &dnsResolver{
		lookup: &fakeSRVLookup{records: []*net.SRV{
			{Target: "mono-mv-2.mono-mv-headless.default.svc."},
			{Target: "mono-mv-0.mono-mv-headless.default.svc."},
			{Target: "mono-mv-1.mono-mv-headless.default.svc."},
		}},
		timeout: time.Second,
	}, MonoVertexRequest("mono", "default", "metrics"), func(index int) bool {
		current := concurrent.Add(1)
		defer concurrent.Add(-1)
		for {
			previous := maxConcurrent.Load()
			if current <= previous || maxConcurrent.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		return index != 1
	})

	require.NoError(t, err)
	assert.Equal(t, []int{0, 2}, active)
	assert.Greater(t, maxConcurrent.Load(), int32(1))
}
