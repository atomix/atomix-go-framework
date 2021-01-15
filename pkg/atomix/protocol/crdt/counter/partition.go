package counter

import (
	"github.com/atomix/go-framework/pkg/atomix/protocol/crdt"
)

func newPartition(parent *crdt.Partition) *Partition {
	return &Partition{
		ID:     parent.ID,
		parent: parent,
	}
}

// Partition is a partition
type Partition struct {
	ID     crdt.PartitionID
	parent *crdt.Partition
}

// GetService gets the service
func (p *Partition) GetService(name string) (Service, error) {
	service, err := p.parent.GetService(ServiceType, name)
	if err != nil {
		return nil, err
	}
	return service.(Service), nil
}
