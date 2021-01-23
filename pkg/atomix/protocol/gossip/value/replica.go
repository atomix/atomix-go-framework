package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	proto "github.com/golang/protobuf/proto"
	"math/rand"
)

func newClient(serviceID gossip.ServiceID, partition *gossip.Partition) (ReplicationClient, error) {
	group, err := gossip.NewPeerGroup(partition, ServiceType, serviceID)
	if err != nil {
		return nil, err
	}
	return &replicationClient{
		group: group,
	}, nil
}

type ReplicationClient interface {
	Bootstrap(ctx context.Context) (*value.Value, error)
	Repair(ctx context.Context, value *value.Value) (*value.Value, error)
	Advertise(ctx context.Context, value *value.Value) error
	Update(ctx context.Context, value *value.Value) error
}
type replicationClient struct {
	group *gossip.PeerGroup
}

func (p *replicationClient) Bootstrap(ctx context.Context) (*value.Value, error) {
	objects, err := p.group.Read(ctx, "")
	if err != nil {
		return nil, err
	}

	var value *value.Value
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

func (p *replicationClient) Repair(ctx context.Context, value *value.Value) (*value.Value, error) {
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

func (p *replicationClient) Advertise(ctx context.Context, value *value.Value) error {
	peers := p.group.Peers()
	peer := peers[rand.Intn(len(peers))]
	peer.Advertise(ctx, "", meta.FromProto(value.ObjectMeta))
	return nil
}

func (p *replicationClient) Update(ctx context.Context, value *value.Value) error {
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

func (s *replicationServer) Read(ctx context.Context, _ string) (*gossip.Object, error) {
	value, err := s.delegate.Read(ctx)
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

func (s *replicationServer) Update(ctx context.Context, object *gossip.Object) error {
	value := &value.Value{}
	err := proto.Unmarshal(object.Value, value)
	if err != nil {
		return err
	}
	return s.delegate.Update(ctx, value)
}

func (s *replicationServer) ReadAll(ctx context.Context, ch chan<- gossip.Object) error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		value, err := s.delegate.Read(ctx)
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

var _ ReplicationServer = &replicationServer{}
