package set

import (
	"context"
	set "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/time"
)

var log = logging.GetLogger("atomix", "protocol", "gossip", "set")

const ServiceType gossip.ServiceType = "Set"

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
    // Size gets the number of elements in the set
    Size(context.Context, *set.SizeRequest) (*set.SizeResponse, error)
    // Contains returns whether the set contains a value
    Contains(context.Context, *set.ContainsRequest) (*set.ContainsResponse, error)
    // Add adds a value to the set
    Add(context.Context, *set.AddRequest) (*set.AddResponse, error)
    // Remove removes a value from the set
    Remove(context.Context, *set.RemoveRequest) (*set.RemoveResponse, error)
    // Clear removes all values from the set
    Clear(context.Context, *set.ClearRequest) (*set.ClearResponse, error)
    // Events listens for set change events
    Events(context.Context, *set.EventsRequest, chan<- set.EventsResponse) error
    // Elements lists all elements in the set
    Elements(context.Context, *set.ElementsRequest, chan<- set.ElementsResponse) error
}
