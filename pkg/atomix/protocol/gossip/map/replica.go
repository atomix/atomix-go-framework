package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	proto "github.com/golang/protobuf/proto"
	"math/rand"

	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	"github.com/atomix/go-framework/pkg/atomix/time"
)

func newClient(serviceID gossip.ServiceID, partition *gossip.Partition, clock time.Clock) (ReplicationClient, error) {
	group, err := gossip.NewPeerGroup(partition, ServiceType, serviceID)
	if err != nil {
		return nil, err
	}
	return &replicationClient{
		group: group,
		clock: clock,
	}, nil
}

type ReplicationClient interface {
	Clock() time.Clock
	Bootstrap(ctx context.Context, ch chan<- _map.Entry) error
	Repair(ctx context.Context, entry *_map.Entry) (*_map.Entry, error)
	Advertise(ctx context.Context, entry *_map.Entry) error
	Update(ctx context.Context, entry *_map.Entry) error
}

type replicationClient struct {
	group *gossip.PeerGroup
	clock time.Clock
}

func (p *replicationClient) Clock() time.Clock {
	return p.clock
}
func (p *replicationClient) Bootstrap(ctx context.Context, ch chan<- _map.Entry) error {
	objectCh := make(chan gossip.Object)
	if err := p.group.ReadAll(ctx, objectCh); err != nil {
		return err
	}
	go func() {
		for object := range objectCh {
			var entry _map.Entry
			err := proto.Unmarshal(object.Value, &entry)
			if err != nil {
				log.Errorf("Bootstrap failed: %v", err)
			} else {
				ch <- entry
			}
		}
	}()
	return nil
}

func (p *replicationClient) Repair(ctx context.Context, entry *_map.Entry) (*_map.Entry, error) {
	objects, err := p.group.Read(ctx, entry.Key.Key)
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		if meta.FromProto(object.ObjectMeta).After(meta.FromProto(entry.Key.ObjectMeta)) {
			err = proto.Unmarshal(object.Value, entry)
			if err != nil {
				return nil, err
			}
		}
	}
	return entry, nil
}

func (p *replicationClient) Advertise(ctx context.Context, entry *_map.Entry) error {
	peers := p.group.Peers()
	peer := peers[rand.Intn(len(peers))]
	peer.Advertise(ctx, entry.Key.Key, meta.FromProto(entry.Key.ObjectMeta))
	return nil
}

func (p *replicationClient) Update(ctx context.Context, entry *_map.Entry) error {
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

var _ ReplicationClient = &replicationClient{}

func newReplica(service Service) ReplicationServer {
	return &replicationServer{
		delegate: service.Delegate(),
	}
}

type ReplicationServer interface {
	gossip.Replica
}

type replicationServer struct {
	delegate Delegate
}

func (s *replicationServer) Read(ctx context.Context, key string) (*gossip.Object, error) {
	entry, err := s.delegate.Read(ctx, key)
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

func (s *replicationServer) Update(ctx context.Context, object *gossip.Object) error {
	entry := &_map.Entry{}
	err := proto.Unmarshal(object.Value, entry)
	if err != nil {
		return err
	}
	return s.delegate.Update(ctx, entry)
}

func (s *replicationServer) ReadAll(ctx context.Context, ch chan<- gossip.Object) error {
	entriesCh := make(chan _map.Entry)
	errCh := make(chan error)
	go func() {
		err := s.delegate.List(ctx, entriesCh)
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

var _ ReplicationServer = &replicationServer{}
