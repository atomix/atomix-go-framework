// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gossip

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"google.golang.org/grpc"
	"io"
	"sync"
)

type MemberID string

func (i MemberID) String() string {
	return string(i)
}

type PeerID string

func (i PeerID) String() string {
	return string(i)
}

func newPeer(group *PeerGroup, replica *cluster.Replica, clock time.Clock) (*Peer, error) {
	peer := &Peer{
		ID:      PeerID(replica.ID),
		group:   group,
		replica: replica,
		clock:   clock,
		objects: make(map[string]Object),
	}
	if err := peer.connect(); err != nil {
		return nil, err
	}
	return peer, nil
}

type Peer struct {
	ID          PeerID
	group       *PeerGroup
	replica     *cluster.Replica
	clock       time.Clock
	client      GossipProtocolClient
	stream      GossipProtocol_GossipClient
	advertiseCh chan Advertise
	updateCh    chan Update
	objects     map[string]Object
	objectsMu   sync.RWMutex
	cancel      context.CancelFunc
}

func (p *Peer) connect() error {
	conn, err := p.replica.Connect(context.Background(), cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		return err
	}
	p.client = NewGossipProtocolClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := p.client.Gossip(ctx)
	if err != nil {
		return err
	}
	p.cancel = cancel
	msg := &GossipMessage{
		Message: &GossipMessage_Initialize{
			Initialize: &Initialize{
				Header: RequestHeader{
					PartitionID: p.group.partition.ID,
					ServiceID:   p.group.serviceID,
					MemberID:    p.group.memberID,
					Timestamp:   p.clock.Scheme().Codec().EncodeTimestamp(p.clock.Increment()),
				},
			},
		},
	}
	log.Debugf("Sending GossipMessage %s->%s %+v", p.group.memberID, p.ID, msg)
	err = stream.Send(msg)
	if err != nil {
		return err
	}

	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				log.Error(err)
				return
			} else {
				log.Debugf("Received GossipMessage %s->%s %+v", p.ID, p.group.memberID, msg)
				replica, err := p.group.partition.getReplica(ctx, p.group.serviceID)
				if err != nil {
					log.Error(err)
					return
				}

				switch m := msg.Message.(type) {
				case *GossipMessage_Advertise:
					p.clock.Update(time.NewTimestamp(m.Advertise.Header.Timestamp))
					object, err := replica.Read(stream.Context(), m.Advertise.Key)
					if err != nil {
						log.Error(err)
						return
					} else if object != nil {
						if meta.FromProto(object.ObjectMeta).After(meta.FromProto(m.Advertise.ObjectMeta)) {
							msg := &GossipMessage{
								Message: &GossipMessage_Update{
									Update: &Update{
										Header: GossipHeader{
											Timestamp: p.clock.Scheme().Codec().EncodeTimestamp(p.clock.Increment()),
										},
										Object: *object,
									},
								},
							}
							log.Debugf("Sending GossipMessage %s->%s %+v", p.group.memberID, p.ID, msg)
							err := stream.Send(msg)
							if err != nil {
								log.Error(err)
								return
							}
						} else if meta.FromProto(m.Advertise.ObjectMeta).After(meta.FromProto(object.ObjectMeta)) {
							msg := &GossipMessage{
								Message: &GossipMessage_Advertise{
									Advertise: &Advertise{
										Header: GossipHeader{
											Timestamp: p.clock.Scheme().Codec().EncodeTimestamp(p.clock.Increment()),
										},
										ObjectMeta: object.ObjectMeta,
										Key:        object.Key,
									},
								},
							}
							log.Debugf("Sending GossipMessage %s->%s %+v", p.group.memberID, p.ID, msg)
							err := stream.Send(msg)
							if err != nil {
								log.Error(err)
								return
							}
						}
					}
				case *GossipMessage_Update:
					p.clock.Update(time.NewTimestamp(m.Update.Header.Timestamp))
					err = replica.Update(stream.Context(), &m.Update.Object)
					if err != nil {
						log.Error(err)
						return
					}
				}
			}
		}
	}()

	p.advertiseCh = make(chan Advertise)
	p.updateCh = make(chan Update)
	go func() {
		for {
			select {
			case advertise := <-p.advertiseCh:
				advertise.Header.Timestamp = p.clock.Scheme().Codec().EncodeTimestamp(p.clock.Increment())
				msg := &GossipMessage{
					Message: &GossipMessage_Advertise{
						Advertise: &advertise,
					},
				}
				log.Debugf("Sending GossipMessage %s->%s %+v", p.group.memberID, p.ID, msg)
				err := stream.Send(msg)
				if err != nil {
					return
				}
			case update := <-p.updateCh:
				update.Header.Timestamp = p.clock.Scheme().Codec().EncodeTimestamp(p.clock.Increment())
				msg := &GossipMessage{
					Message: &GossipMessage_Update{
						Update: &update,
					},
				}
				log.Debugf("Sending GossipMessage %s->%s %+v", p.group.memberID, p.ID, msg)
				err := stream.Send(msg)
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (p *Peer) Read(ctx context.Context, key string) (*Object, error) {
	request := &ReadRequest{
		Header: RequestHeader{
			PartitionID: p.group.partition.ID,
			ServiceID:   p.group.serviceID,
			MemberID:    p.group.memberID,
			Timestamp:   p.clock.Scheme().Codec().EncodeTimestamp(p.clock.Get()),
		},
		Key: key,
	}
	log.Debugf("Sending ReadRequest %s->%s %+v", p.group.memberID, p.ID, request)
	response, err := p.client.Read(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	log.Debugf("Received ReadResponse %s-%s %+v", p.ID, p.group.memberID, response)
	p.clock.Update(time.NewTimestamp(response.Header.Timestamp))
	return response.Object, nil
}

func (p *Peer) ReadAll(ctx context.Context, ch chan<- Object) error {
	request := &ReadAllRequest{
		Header: RequestHeader{
			PartitionID: p.group.partition.ID,
			ServiceID:   p.group.serviceID,
			MemberID:    p.group.memberID,
			Timestamp:   p.clock.Scheme().Codec().EncodeTimestamp(p.clock.Get()),
		},
	}
	log.Debugf("Sending ReadAllRequest %s->%s %+v", p.group.memberID, p.ID, request)
	stream, err := p.client.ReadAll(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	go func() {
		defer close(ch)
		for {
			response, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					log.Error(err)
				}
				return
			} else {
				log.Debugf("Received ReadAllResponse %s-%s %+v", p.ID, p.group.memberID, response)
				p.clock.Update(time.NewTimestamp(response.Header.Timestamp))
				ch <- response.Object
			}
		}
	}()
	return nil
}

func (p *Peer) Advertise(ctx context.Context, key string, digest meta.ObjectMeta) {
	p.advertiseCh <- Advertise{
		ObjectMeta: digest.Proto(),
		Key:        key,
	}
}

func (p *Peer) Update(ctx context.Context, object *Object) {
	p.updateCh <- Update{
		Object: *object,
	}
}

func (p *Peer) close() {
	p.cancel()
}
