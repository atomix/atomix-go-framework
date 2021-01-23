package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	proto "github.com/golang/protobuf/proto"

	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
)

type Protocol interface {
	RepairRead(ctx context.Context, entry *_map.Entry) (*_map.Entry, error)
	BroadcastUpdate(ctx context.Context, entry *_map.Entry) error
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

func (p *serviceProtocol) RepairRead(ctx context.Context, entry *_map.Entry) (*_map.Entry, error) {
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

func (p *serviceProtocol) BroadcastUpdate(ctx context.Context, entry *_map.Entry) error {
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

var _ Protocol = &serviceProtocol{}
