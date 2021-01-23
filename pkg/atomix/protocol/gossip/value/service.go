package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
)

const ServiceType gossip.ServiceType = "Value"

// RegisterService registers the service on the given node
func RegisterService(node *gossip.Node) {
	node.RegisterService(ServiceType, func(serviceID gossip.ServiceID, partition *gossip.Partition) (gossip.Replica, error) {
		protocol, err := newProtocol(serviceID, partition)
		if err != nil {
			return nil, err
		}
		return newReplicaAdaptor(newReplica(protocol)), nil
	})
}

type Service interface {
	// Set sets the value
	Set(context.Context, *value.SetRequest) (*value.SetResponse, error)
	// Get gets the value
	Get(context.Context, *value.GetRequest) (*value.GetResponse, error)
	// Events listens for value change events
	Events(context.Context, *value.EventsRequest, chan<- value.EventsResponse) error
}
