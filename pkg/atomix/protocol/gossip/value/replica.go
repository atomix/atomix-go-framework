package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	proto "github.com/golang/protobuf/proto"
)

var newReplica func(protocol Protocol) Replica

func registerReplica(f func(protocol Protocol) Replica) {
	newReplica = f
}

type Replica interface {
	Service() Service
	Read(ctx context.Context) (*value.Value, error)
	Update(ctx context.Context, value *value.Value) error
}
type serviceReplica struct {
	replica Replica
}

func (s *serviceReplica) Service() gossip.Service {
	return s.replica
}

func (s *serviceReplica) Read(ctx context.Context, _ string) (*gossip.Object, error) {
	value, err := s.replica.Read(ctx)
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

func (s *serviceReplica) Update(ctx context.Context, object *gossip.Object) error {
	value := &value.Value{}
	err := proto.Unmarshal(object.Value, value)
	if err != nil {
		return err
	}
	return s.replica.Update(ctx, value)
}

func (s *serviceReplica) ReadAll(ctx context.Context, ch chan<- gossip.Object) error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		value, err := s.replica.Read(ctx)
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

var _ gossip.Replica = &serviceReplica{}
