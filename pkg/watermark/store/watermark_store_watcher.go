package store

// watermarkStoreWatcher defines a pair of heartbeatStoreWatcher and offsetTimelineStoreWatcher,
// it implements interface WatermarkStoreWatcher
type watermarkStoreWatcher struct {
	heartbeatStoreWatcher      WatermarkKVWatcher
	offsetTimelineStoreWatcher WatermarkKVWatcher
}

var _ WatermarkStoreWatcher = (*watermarkStoreWatcher)(nil)

func (w *watermarkStoreWatcher) HeartbeatWatcher() WatermarkKVWatcher {
	return w.heartbeatStoreWatcher
}
func (w *watermarkStoreWatcher) OffsetTimelineWatcher() WatermarkKVWatcher {
	return w.offsetTimelineStoreWatcher
}

// BuildWatermarkStoreWatcher returns a WatermarkStoreWatcher instance
func BuildWatermarkStoreWatcher(hbStoreWatcher, otStoreWatcher WatermarkKVWatcher) WatermarkStoreWatcher {
	return &watermarkStoreWatcher{
		heartbeatStoreWatcher:      hbStoreWatcher,
		offsetTimelineStoreWatcher: otStoreWatcher,
	}
}
