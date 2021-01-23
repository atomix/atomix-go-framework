package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
)

const ServiceType gossip.ServiceType = "Value"

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
	Read(ctx context.Context) (*value.Value, error)
	Update(ctx context.Context, value *value.Value) error
}

type Service interface {
	gossip.Service
	Delegate() Delegate
	// Set sets the value
	Set(context.Context, *value.SetRequest) (*value.SetResponse, error)
	// Get gets the value
	Get(context.Context, *value.GetRequest) (*value.GetResponse, error)
	// Events listens for value change events
	Events(context.Context, *value.EventsRequest, chan<- value.EventsResponse) error
}
