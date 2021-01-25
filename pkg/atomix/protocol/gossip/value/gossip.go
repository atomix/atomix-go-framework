package value

import (
	"context"
	"math/rand"
	"time"

	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	atime "github.com/atomix/go-framework/pkg/atomix/time"

	"github.com/golang/protobuf/proto"
)

const antiEntropyPeriod = time.Second

func newGossipProtocol(serviceID gossip.ServiceID, partition *gossip.Partition, clock atime.Clock) (GossipProtocol, error) {
	peers, err := gossip.NewPeerGroup(partition, ServiceType, serviceID)
	if err != nil {
		return nil, err
	}
	return &gossipProtocol{
		clock:  clock,
		group:  newGossipGroup(peers),
		server: newGossipServer(partition),
	}, nil
}

type GossipProtocol interface {
	Clock() atime.Clock
	Group() GossipGroup
	Server() GossipServer
}

type GossipHandler interface {
	Read(ctx context.Context) (*ValueState, error)
	Update(ctx context.Context, state *ValueState) error
}

type GossipServer interface {
	Register(GossipHandler) error
	handler() GossipHandler
}

type GossipClient interface {
	Bootstrap(ctx context.Context) (*ValueState, error)
	Repair(ctx context.Context, state *ValueState) (*ValueState, error)
	Advertise(ctx context.Context, state *ValueState) error
	Update(ctx context.Context, state *ValueState) error
}

type GossipGroup interface {
	GossipClient
	MemberID() GossipMemberID
	Members() []GossipMember
	Member(GossipMemberID) GossipMember
}

type GossipMemberID gossip.PeerID

func (i GossipMemberID) String() string {
    return string(i)
}

type GossipMember interface {
	GossipClient
	ID() GossipMemberID
	Client() *gossip.Peer
}

type gossipProtocol struct {
	clock  atime.Clock
	group  GossipGroup
	server GossipServer
}

func (p *gossipProtocol) Clock() atime.Clock {
	return p.clock
}

func (p *gossipProtocol) Group() GossipGroup {
	return p.group
}

func (p *gossipProtocol) Server() GossipServer {
	return p.server
}

var _ GossipProtocol = &gossipProtocol{}

func newGossipGroup(group *gossip.PeerGroup) GossipGroup {
	peers := group.Peers()
	members := make([]GossipMember, 0, len(peers))
	memberIDs := make(map[GossipMemberID]GossipMember)
	for _, peer := range peers {
		member := newGossipMember(peer)
		members = append(members, member)
		memberIDs[member.ID()] = member
	}
	return &gossipGroup{
		group: group,
	}
}

type gossipGroup struct {
	group     *gossip.PeerGroup
	members   []GossipMember
	memberIDs map[GossipMemberID]GossipMember
}

func (p *gossipGroup) MemberID() GossipMemberID {
    return GossipMemberID(p.group.MemberID())
}

func (p *gossipGroup) Members() []GossipMember {
	return p.members
}

func (p *gossipGroup) Member(id GossipMemberID) GossipMember {
	return p.memberIDs[id]
}
func (p *gossipGroup) Bootstrap(ctx context.Context) (*ValueState, error) {
	objects, err := p.group.Read(ctx, "")
	if err != nil {
		return nil, err
	}

    state := &ValueState{}
	for _, object := range objects {
		if meta.FromProto(object.ObjectMeta).After(meta.FromProto(state.ObjectMeta)) {
			err = proto.Unmarshal(object.Value, state)
			if err != nil {
				return nil, err
			}
		}
	}
	return state, nil
}

func (p *gossipGroup) Repair(ctx context.Context, state *ValueState) (*ValueState, error) {
	objects, err := p.group.Read(ctx, "")
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		if meta.FromProto(object.ObjectMeta).After(meta.FromProto(state.ObjectMeta)) {
			err = proto.Unmarshal(object.Value, state)
			if err != nil {
				return nil, err
			}
		}
	}
	return state, nil
}

func (p *gossipGroup) Advertise(ctx context.Context, state *ValueState) error {
	peers := p.group.Peers()
	peer := peers[rand.Intn(len(peers))]
	peer.Advertise(ctx, "", meta.FromProto(state.ObjectMeta))
	return nil
}

func (p *gossipGroup) Update(ctx context.Context, state *ValueState) error {
	bytes, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	object := &gossip.Object{
		ObjectMeta: state.ObjectMeta,
		Value:      bytes,
	}
	p.group.Update(ctx, object)
	return nil
}

var _ GossipGroup = &gossipGroup{}

func newGossipServer(partition *gossip.Partition) GossipServer {
	return &gossipServer{
		partition: partition,
	}
}

type gossipServer struct {
	partition     *gossip.Partition
	gossipHandler GossipHandler
}

func (s *gossipServer) Register(handler GossipHandler) error {
	s.gossipHandler = handler
	return s.partition.RegisterReplica(newReplica(handler))
}

func (s *gossipServer) handler() GossipHandler {
	return s.gossipHandler
}

var _ GossipServer = &gossipServer{}

func newGossipMember(peer *gossip.Peer) GossipMember {
	return &gossipMember{
		id:   GossipMemberID(peer.ID),
		peer: peer,
	}
}

type gossipMember struct {
	id   GossipMemberID
	peer *gossip.Peer
}

func (p *gossipMember) ID() GossipMemberID {
	return p.id
}

func (p *gossipMember) Client() *gossip.Peer {
	return p.peer
}
func (p *gossipMember) Bootstrap(ctx context.Context) (*ValueState, error) {
	object, err := p.peer.Read(ctx, "")
	if err != nil {
		return nil, err
	}

    state := &ValueState{}
    err = proto.Unmarshal(object.Value, state)
    if err != nil {
        return nil, err
    }
	return state, nil
}

func (p *gossipMember) Repair(ctx context.Context, state *ValueState) (*ValueState, error) {
	object, err := p.peer.Read(ctx, "")
	if err != nil {
		return nil, err
	}
    if meta.FromProto(object.ObjectMeta).After(meta.FromProto(state.ObjectMeta)) {
        err = proto.Unmarshal(object.Value, state)
        if err != nil {
            return nil, err
        }
	}
	return state, nil
}

func (p *gossipMember) Advertise(ctx context.Context, state *ValueState) error {
	p.peer.Advertise(ctx, "", meta.FromProto(state.ObjectMeta))
	return nil
}

func (p *gossipMember) Update(ctx context.Context, state *ValueState) error {
	bytes, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	object := &gossip.Object{
		ObjectMeta: state.ObjectMeta,
		Value:      bytes,
	}
	p.peer.Update(ctx, object)
	return nil
}

var _ GossipMember = &gossipMember{}

func newReplica(handler GossipHandler) gossip.Replica {
	return &gossipReplica{
		handler: handler,
	}
}

type gossipReplica struct {
	id      gossip.ServiceID
	handler GossipHandler
}

func (s *gossipReplica) ID() gossip.ServiceID {
	return s.id
}

func (s *gossipReplica) Type() gossip.ServiceType {
	return ServiceType
}

func (s *gossipReplica) Update(ctx context.Context, object *gossip.Object) error {
	state := &ValueState{}
	err := proto.Unmarshal(object.Value, state)
	if err != nil {
		return err
	}
	return s.handler.Update(ctx, state)
}

func (s *gossipReplica) Read(ctx context.Context, key string) (*gossip.Object, error) {
	state, err := s.handler.Read(ctx)
	if err != nil {
		return nil, err
	} else if state == nil {
		return nil, nil
	}

	bytes, err := proto.Marshal(state)
	if err != nil {
		return nil, err
	}
	return &gossip.Object{
        ObjectMeta: state.ObjectMeta,
		Value:      bytes,
	}, nil
}

func (s *gossipReplica) ReadAll(ctx context.Context, ch chan<- gossip.Object) error {
    errCh := make(chan error)
    go func() {
        defer close(errCh)
        state, err := s.handler.Read(ctx)
        if err != nil {
            errCh <- err
            return
        }
        bytes, err := proto.Marshal(state)
        if err != nil {
            errCh <- err
            return
        }
        object := gossip.Object{
            ObjectMeta: state.ObjectMeta,
            Value:      bytes,
        }
        ch <- object
    }()
	return <-errCh
}

var _ gossip.Replica = &gossipReplica{}

type GossipEngine interface {
	start()
	stop()
}

func newGossipEngine(protocol GossipProtocol) GossipEngine {
	return &gossipEngine{
		protocol: protocol,
	}
}

type gossipEngine struct {
	protocol GossipProtocol
	ticker   *time.Ticker
	cancel   context.CancelFunc
}

func (m *gossipEngine) start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	if err := m.bootstrap(ctx); err != nil {
		log.Errorf("Failed to bootstrap service: %v", err)
	}
	m.runAntiEntropy(ctx)
}

func (m *gossipEngine) bootstrap(ctx context.Context) error {
    state, err := m.protocol.Group().Bootstrap(ctx)
    if err != nil {
        return err
    }
    if err := m.protocol.Server().handler().Update(ctx, state); err != nil {
        return err
    }
	return nil
}

func (m *gossipEngine) runAntiEntropy(ctx context.Context) {
	m.ticker = time.NewTicker(antiEntropyPeriod)
	for range m.ticker.C {
		if err := m.advertise(ctx); err != nil {
			log.Errorf("Anti-entropy protocol failed: %v", err)
		}
	}
}

func (m *gossipEngine) advertise(ctx context.Context) error {
    state, err := m.protocol.Server().handler().Read(context.Background())
    if err != nil {
        return err
    }
    if err := m.protocol.Group().Advertise(ctx, state); err != nil {
        return err
    }
	return nil
}

func (m *gossipEngine) stop() {
	m.ticker.Stop()
	m.cancel()
}
