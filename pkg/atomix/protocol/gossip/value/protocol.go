package value

import (
	"context"
	value "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	proto "github.com/golang/protobuf/proto"
)

type Protocol interface {
	RepairRead(ctx context.Context, value *value.Value) (*value.Value, error)
	BroadcastUpdate(ctx context.Context, value *value.Value) error
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

func (p *serviceProtocol) RepairRead(ctx context.Context, value *value.Value) (*value.Value, error) {
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

func (p *serviceProtocol) BroadcastUpdate(ctx context.Context, value *value.Value) error {
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

var _ Protocol = &serviceProtocol{}
