package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
)

const ServiceType gossip.ServiceType = "Map"

// RegisterService registers the service on the given node
func RegisterService(node *gossip.Node) {
	node.RegisterService(ServiceType, func(serviceID gossip.ServiceID, partition *gossip.Partition) (gossip.Service, error) {
		client, err := newClient(serviceID, partition)
		if err != nil {
			return nil, err
		}
		return newService(client), nil
	})
}

var newService func(replicas ReplicationClient) Service

func registerService(f func(replicas ReplicationClient) Service) {
	newService = f
}

type Delegate interface {
	Read(ctx context.Context, key string) (*_map.Entry, error)
	List(ctx context.Context, ch chan<- _map.Entry) error
	Update(ctx context.Context, entry *_map.Entry) error
}

type Service interface {
	gossip.Service
	Delegate() Delegate
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
