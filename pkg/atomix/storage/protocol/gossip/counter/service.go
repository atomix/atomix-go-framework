package counter

import (
	"context"
	counter "github.com/atomix/api/go/atomix/primitive/counter"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip"
	"github.com/atomix/go-framework/pkg/atomix/time"
)

var log = logging.GetLogger("atomix", "protocol", "gossip", "counter")

const ServiceType gossip.ServiceType = "Counter"

// RegisterService registers the service on the given node
func RegisterService(node *gossip.Node) {
	node.RegisterService(ServiceType, func(ctx context.Context, serviceID gossip.ServiceId, partition *gossip.Partition, clock time.Clock) (gossip.Service, error) {
		protocol, err := newGossipProtocol(serviceID, partition, clock)
		if err != nil {
			return nil, err
		}
		service, err := newService(protocol)
		if err != nil {
			return nil, err
		}
		engine := newGossipEngine(protocol)
		go engine.start()
		return service, nil
	})
}

var newService func(protocol GossipProtocol) (Service, error)

func registerService(f func(protocol GossipProtocol) (Service, error)) {
	newService = f
}

type Service interface {
	gossip.Service
	// Set sets the counter value
	Set(context.Context, *counter.SetRequest) (*counter.SetResponse, error)
	// Get gets the current counter value
	Get(context.Context, *counter.GetRequest) (*counter.GetResponse, error)
	// Increment increments the counter value
	Increment(context.Context, *counter.IncrementRequest) (*counter.IncrementResponse, error)
	// Decrement decrements the counter value
	Decrement(context.Context, *counter.DecrementRequest) (*counter.DecrementResponse, error)
}
