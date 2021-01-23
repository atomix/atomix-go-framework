package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	proto "github.com/golang/protobuf/proto"

	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
)

var newReplica func(protocol Protocol) Replica

func registerReplica(f func(protocol Protocol) Replica) {
	newReplica = f
}

type Replica interface {
	Service() Service
	Read(ctx context.Context, key string) (*_map.Entry, error)
	List(ctx context.Context, ch chan<- _map.Entry) error
	Update(ctx context.Context, entry *_map.Entry) error
}

func newReplicaAdaptor(replica Replica) gossip.Replica {
	return &serviceReplicaAdaptor{
		replica: replica,
	}
}

type serviceReplicaAdaptor struct {
	replica Replica
}

func (s *serviceReplicaAdaptor) Service() gossip.Service {
	return s.replica
}

func (s *serviceReplicaAdaptor) Read(ctx context.Context, key string) (*gossip.Object, error) {
	entry, err := s.replica.Read(ctx, key)
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

func (s *serviceReplicaAdaptor) Update(ctx context.Context, object *gossip.Object) error {
	entry := &_map.Entry{}
	err := proto.Unmarshal(object.Value, entry)
	if err != nil {
		return err
	}
	return s.replica.Update(ctx, entry)
}

func (s *serviceReplicaAdaptor) ReadAll(ctx context.Context, ch chan<- gossip.Object) error {
	entriesCh := make(chan _map.Entry)
	errCh := make(chan error)
	go func() {
		err := s.replica.List(ctx, entriesCh)
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

var _ gossip.Replica = &serviceReplicaAdaptor{}
