package pbq

// Manager helps in managing PBQ instances
type Manager struct {
	// options
}

// ListPartitions returns all the partitions from the store
func ListPartitions() []*PBQ {
	return nil
}

// PBQForPartition return a pbq for the supplied partition identifer
// If such a partition is not known, a new PBQ will be created and returned.
func PBQForPartition(partitionid string) *PBQ {
	return nil
}
