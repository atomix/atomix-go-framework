package {{ .Package.Name }}

import (
    "context"
	"github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/protocol/crdt"
)

// newManager creates a new manager
func newManager(parent *crdt.Manager) *Manager {
	return &Manager{
		parent: parent,
	}
}

// Manager is a primitive partition manager
type Manager struct {
	parent *crdt.Manager
}

func (m *Manager) PartitionFrom(ctx context.Context) (*Partition, error) {
	partition, err := m.parent.PartitionFrom(ctx)
	if err != nil {
		return nil, err
	}
	return newPartition(partition), nil
}

func (m *Manager) PartitionsFrom(ctx context.Context) ([]*Partition, error) {
	parents, err := m.parent.PartitionsFrom(ctx)
	if err != nil {
		return nil, err
	}
	partitions := make([]*Partition, len(parents))
	for i, parent := range parents {
		partitions[i] = newPartition(parent)
	}
	return partitions, nil
}

func (m *Manager) Partition(partitionID crdt.PartitionID) (*Partition, error) {
	parent, err := m.parent.Partition(partitionID)
	if err != nil {
		return nil, err
	}
	return newPartition(parent), nil
}

func (m *Manager) PartitionBy(partitionKey []byte) (*Partition, error) {
	parent, err := m.parent.PartitionBy(partitionKey)
	if err != nil {
		return nil, err
	}
	return newPartition(parent), nil
}

func (m *Manager) PartitionFor(primitiveID primitive.PrimitiveId) (*Partition, error) {
	parent, err := m.parent.PartitionFor(primitiveID)
	if err != nil {
		return nil, err
	}
	return newPartition(parent), nil
}
