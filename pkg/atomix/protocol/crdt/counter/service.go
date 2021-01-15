package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/protocol/crdt"
)

const ServiceType crdt.ServiceType = "Counter"

// RegisterService registers the service on the given node
func RegisterService(node *crdt.Node) {
	node.RegisterService(ServiceType, newServiceFunc)
}

var newServiceFunc crdt.NewServiceFunc

type Service interface {
	crdt.Service
	// Set sets the counter value
	Set(context.Context, *counter.SetInput) (*counter.SetOutput, error)
	// Get gets the current counter value
	Get(context.Context, *counter.GetInput) (*counter.GetOutput, error)
	// Increment increments the counter value
	Increment(context.Context, *counter.IncrementInput) (*counter.IncrementOutput, error)
	// Decrement decrements the counter value
	Decrement(context.Context, *counter.DecrementInput) (*counter.DecrementOutput, error)
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context) (*counter.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(context.Context, *counter.Snapshot) error
}
