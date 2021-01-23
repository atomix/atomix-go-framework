package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	proto "github.com/golang/protobuf/proto"
)

const ServiceType gossip.ServiceType = "Value"

// RegisterService registers the service on the given node
func RegisterService(node *gossip.Node) {
	node.RegisterService(ServiceType, func(serviceID gossip.ServiceID, partition *gossip.Partition) (gossip.Replica, error) {
		protocol, err := newProtocol(serviceID, partition)
		if err != nil {
			return nil, err
		}
		service := newServiceFunc(protocol)
		return &gossipReplica{service: service}, nil
	})
}

var newServiceFunc func(protocol Protocol) Service

func registerService(f func(protocol Protocol) Service) {
	newServiceFunc = f
}

type Protocol interface {
	Repair(ctx context.Context, value *value.Value) (*value.Value, error)
	Broadcast(ctx context.Context, value *value.Value) error
}

func newProtocol(serviceID gossip.ServiceID, partition *gossip.Partition) (Protocol, error) {
	group, err := gossip.NewPeerGroup(partition, ServiceType, serviceID)
	if err != nil {
		return nil, err
	}
	protocol := &serviceProtocol{
		group: group,
	}
	return protocol, nil
}

type serviceProtocol struct {
	group *gossip.PeerGroup
}

func (p *serviceProtocol) Repair(ctx context.Context, value *value.Value) (*value.Value, error) {
	objects, err := p.group.Read(ctx, "")
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		if meta.FromProto(object.ObjectMeta).After(meta.FromProto(value.ObjectMeta)) {
			err = proto.Unmarshal(object.Value, value)
			if err != nil {
				return nil, err
			}
		}
	}
	return value, nil
}

func (p *serviceProtocol) Broadcast(ctx context.Context, value *value.Value) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	object := &gossip.Object{
		ObjectMeta: value.ObjectMeta,
		Value:      bytes,
	}
	p.group.Update(ctx, object)
	return nil
}

type gossipReplica struct {
	service Service
}

func (s *gossipReplica) Service() gossip.Service {
	return s.service
}

func (s *gossipReplica) Read(ctx context.Context, _ string) (*gossip.Object, error) {
	value, err := s.service.Read(ctx)
	if err != nil {
		return nil, err
	} else if value == nil {
		return nil, nil
	}

	bytes, err := proto.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &gossip.Object{
		ObjectMeta: value.ObjectMeta,
		Value:      bytes,
	}, nil
}

func (s *gossipReplica) Update(ctx context.Context, object *gossip.Object) error {
	value := &value.Value{}
	err := proto.Unmarshal(object.Value, value)
	if err != nil {
		return err
	}
	return s.service.Update(ctx, value)
}

func (s *gossipReplica) Clone(ctx context.Context, ch chan<- gossip.Object) error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		value, err := s.service.Read(ctx)
		if err != nil {
			errCh <- err
			return
		}
		bytes, err := proto.Marshal(value)
		if err != nil {
			errCh <- err
			return
		}
		object := gossip.Object{
			ObjectMeta: value.ObjectMeta,
			Value:      bytes,
		}
		ch <- object
	}()
	return <-errCh
}

var _ gossip.Replica = &gossipReplica{}

type Replica interface {
	Protocol() Protocol
	Read(ctx context.Context) (*value.Value, error)
	Update(ctx context.Context, value *value.Value) error
}

type Service interface {
	Replica
	// Set sets the value
	Set(context.Context, *value.SetRequest) (*value.SetResponse, error)
	// Get gets the value
	Get(context.Context, *value.GetRequest) (*value.GetResponse, error)
	// Events listens for value change events
	Events(context.Context, *value.EventsRequest, chan<- value.EventsResponse) error
}
