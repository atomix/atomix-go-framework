package _map

import (
	"github.com/atomix/go-framework/pkg/atomix/protocol/p2p"
)

func newPartition(parent *p2p.Partition) *Partition {
	return &Partition{
		ID:     parent.ID,
		parent: parent,
	}
}

// Partition is a partition
type Partition struct {
	ID     p2p.PartitionID
	parent *p2p.Partition
}

// GetService gets the service
func (p *Partition) GetService(name string) (Service, error) {
	service, err := p.parent.GetService(ServiceType, name)
	if err != nil {
		return nil, err
	}
	return service.(Service), nil
}