package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	"github.com/golang/protobuf/proto"
)

const ServiceType gossip.ServiceType = "Map"

// RegisterService registers the service on the given node
func RegisterService(node *gossip.Node) {
	node.RegisterService(ServiceType, func(serviceID gossip.ServiceID, partition *gossip.Partition) (gossip.Service, error) {
		protocol, err := newProtocol(serviceID, partition)
		if err != nil {
			return nil, err
		}
		service := newServiceFunc(protocol)
		return &gossipService{service: service}, nil
	})
}

var newServiceFunc func(protocol Protocol) Service

func registerService(f func(protocol Protocol) Service) {
	newServiceFunc = f
}

type Protocol interface {
	Repair(ctx context.Context, entry *_map.Entry) (*_map.Entry, error)
	Broadcast(ctx context.Context, entry *_map.Entry) error
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

func (p *serviceProtocol) Repair(ctx context.Context, entry *_map.Entry) (*_map.Entry, error) {
	objects, err := p.group.Read(ctx, entry.Key.Key)
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		if meta.New(object.ObjectMeta).After(meta.New(entry.Key.ObjectMeta)) {
			err = proto.Unmarshal(object.Value, entry)
			if err != nil {
				return nil, err
			}
		}
	}
	return entry, nil
}

func (p *serviceProtocol) Broadcast(ctx context.Context, entry *_map.Entry) error {
	bytes, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	object := &gossip.Object{
		ObjectMeta: entry.Key.ObjectMeta,
		Value:      bytes,
	}
	p.group.Update(ctx, object)
	return nil
}

type gossipService struct {
	service Service
}

func (s *gossipService) Read(ctx context.Context, key string) (*gossip.Object, error) {
	entry, err := s.service.Read(ctx, key)
	if err != nil {
		return nil, err
	} else if entry == nil {
		return nil, nil
	}

	bytes, err := proto.Marshal(entry)
	if err != nil {
		return nil, err
	}
	return &gossip.Object{
		ObjectMeta: entry.Key.ObjectMeta,
		Key:        entry.Key.Key,
		Value:      bytes,
	}, nil
}

func (s *gossipService) Update(ctx context.Context, object *gossip.Object) error {
	entry := &_map.Entry{}
	err := proto.Unmarshal(object.Value, entry)
	if err != nil {
		return err
	}
	return s.service.Update(ctx, entry)
}

func (s *gossipService) Clone(ctx context.Context, ch chan<- gossip.Object) error {
	entriesCh := make(chan _map.Entry)
	errCh := make(chan error)
	go func() {
		err := s.service.List(ctx, entriesCh)
		if err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer close(errCh)
		for entry := range entriesCh {
			bytes, err := proto.Marshal(&entry)
			if err != nil {
				errCh <- err
				return
			}
			object := gossip.Object{
				ObjectMeta: entry.Key.ObjectMeta,
				Key:        entry.Key.Key,
				Value:      bytes,
			}
			ch <- object
		}
	}()
	return <-errCh
}

var _ gossip.Service = &gossipService{}

type Replica interface {
	Protocol() Protocol
	Read(ctx context.Context, key string) (*_map.Entry, error)
	List(ctx context.Context, ch chan<- _map.Entry) error
	Update(ctx context.Context, entry *_map.Entry) error
}

type Service interface {
	Replica
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
