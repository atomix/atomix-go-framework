package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/protocol/p2p"
)

const ServiceType p2p.ServiceType = "Counter"

// RegisterService registers the service on the given node
func RegisterService(node *p2p.Node) {
	node.RegisterService(ServiceType, newServiceFunc)
}

var newServiceFunc p2p.NewServiceFunc

type Service interface {
	p2p.Service
	// Set sets the counter value
	Set(context.Context, *counter.SetInput) (*counter.SetOutput, error)
	// Get gets the current counter value
	Get(context.Context, *counter.GetInput) (*counter.GetOutput, error)
	// Increment increments the counter value
	Increment(context.Context, *counter.IncrementInput) (*counter.IncrementOutput, error)
	// Decrement decrements the counter value
	Decrement(context.Context, *counter.DecrementInput) (*counter.DecrementOutput, error)
	// CheckAndSet performs a check-and-set operation on the counter value
	CheckAndSet(context.Context, *counter.CheckAndSetInput) (*counter.CheckAndSetOutput, error)
	// Snapshot exports a snapshot of the primitive state
	Snapshot(context.Context) (*counter.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(context.Context, *counter.Snapshot) error
}
