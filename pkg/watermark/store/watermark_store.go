package store

// watermarkStore wraps a pair of heartbeatStore and offsetTimelineStore,
// it implements interface WatermarkStorers.
type watermarkStore struct {
	heartbeatStore      WatermarkKVStorer
	offsetTimelineStore WatermarkKVStorer
}

var _ WatermarkStorer = (*watermarkStore)(nil)

// BuildWatermarkStore returns a WatermarkStorer instance
func BuildWatermarkStore(hbStore, otStore WatermarkKVStorer) WatermarkStorer {
	return &watermarkStore{
		heartbeatStore:      hbStore,
		offsetTimelineStore: otStore,
	}
}

func (ws *watermarkStore) HeartbeatStore() WatermarkKVStorer {
	return ws.heartbeatStore
}

func (ws *watermarkStore) OffsetTimelineStore() WatermarkKVStorer {
	return ws.offsetTimelineStore
}
