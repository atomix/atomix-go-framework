package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	"github.com/atomix/go-framework/pkg/atomix/time"
)

var log = logging.GetLogger("atomix", "protocol", "gossip", "value")

const ServiceType gossip.ServiceType = "Value"

// RegisterService registers the service on the given node
func RegisterService(node *gossip.Node) {
	node.RegisterService(ServiceType, func(ctx context.Context, serviceID gossip.ServiceID, partition *gossip.Partition, clock time.Clock) (gossip.Service, error) {
		client, err := newClient(serviceID, partition, clock)
		if err != nil {
			return nil, err
		}
		service := newService(client)
		manager := newManager(client, service)
		go manager.start()
		return service, nil
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
