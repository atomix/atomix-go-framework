package _map

import (
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
)

func newPartition(parent *gossip.Partition) *Partition {
	return &Partition{
		ID:     parent.ID,
		parent: parent,
	}
}

// Partition is a partition
type Partition struct {
	ID     gossip.PartitionID
	parent *gossip.Partition
}

// GetService gets the service
func (p *Partition) GetService(name string) (Service, error) {
	service, err := p.parent.GetService(ServiceType, name)
	if err != nil {
		return nil, err
	}
	return service.(Service), nil
}
