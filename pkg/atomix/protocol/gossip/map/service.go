package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/time"
)

var log = logging.GetLogger("atomix", "protocol", "gossip", "map")

const ServiceType gossip.ServiceType = "Map"

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
    // Size returns the size of the map
    Size(context.Context, *_map.SizeRequest) (*_map.SizeResponse, error)
    // Put puts an entry into the map
    Put(context.Context, *_map.PutRequest) (*_map.PutResponse, error)
    // Get gets the entry for a key
    Get(context.Context, *_map.GetRequest) (*_map.GetResponse, error)
    // Remove removes an entry from the map
    Remove(context.Context, *_map.RemoveRequest) (*_map.RemoveResponse, error)
    // Clear removes all entries from the map
    Clear(context.Context, *_map.ClearRequest) (*_map.ClearResponse, error)
    // Events listens for change events
    Events(context.Context, *_map.EventsRequest, chan<- _map.EventsResponse) error
    // Entries lists all entries in the map
    Entries(context.Context, *_map.EntriesRequest, chan<- _map.EntriesResponse) error
}
