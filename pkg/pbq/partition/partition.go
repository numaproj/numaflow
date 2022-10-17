package partition

import (
	"fmt"
	"time"
)

// ID uniquely identifies a partition
type ID struct {
	Start time.Time
	End   time.Time
	Key   string
}

func (p ID) String() string {
	return fmt.Sprintf("%v-%v-%s", p.Start.Unix(), p.End.Unix(), p.Key)
}
