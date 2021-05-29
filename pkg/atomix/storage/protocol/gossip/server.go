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
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"io"
	"strconv"
)

func RegisterGossipServer(server *grpc.Server, manager *Manager) {
	RegisterGossipProtocolServer(server, newGossipServer(manager))
}

func newGossipServer(manager *Manager) GossipProtocolServer {
	return &GossipServer{
		manager: manager,
	}
}

type GossipServer struct {
	manager *Manager
}

func (s *GossipServer) Gossip(stream GossipProtocol_GossipServer) error {
	member, _ := s.manager.Cluster.Member()
	localID := MemberID(member.ID)
	msg, err := stream.Recv()
	if err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}

	init := msg.GetInitialize()
	senderID := init.Header.MemberID
	log.Debugf("Received GossipMessage %s->%s %+v", senderID, localID, msg)
	replica, err := s.getReplica(stream.Context(), init.Header.PartitionID, init.Header.ServiceID, &init.Header.Timestamp)
	if err != nil {
		return errors.Proto(err)
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		log.Debugf("Received GossipMessage %s->%s %+v", senderID, localID, msg)
		switch m := msg.Message.(type) {
		case *GossipMessage_Advertise:
			replica.Clock().Update(time.NewTimestamp(m.Advertise.Header.Timestamp))
			object, err := replica.Read(stream.Context(), m.Advertise.Key)
			if err != nil {
				return err
			} else if object != nil {
				if meta.FromProto(object.ObjectMeta).After(meta.FromProto(m.Advertise.ObjectMeta)) {
					msg := &GossipMessage{
						Message: &GossipMessage_Update{
							Update: &Update{
								Header: GossipHeader{
									Timestamp: replica.Clock().Scheme().Codec().EncodeTimestamp(replica.Clock().Increment()),
								},
								Object: *object,
							},
						},
					}
					log.Debugf("Sending GossipMessage %s->%s %+v", localID, senderID, msg)
					err := stream.Send(msg)
					if err != nil {
						return err
					}
				} else if meta.FromProto(m.Advertise.ObjectMeta).After(meta.FromProto(object.ObjectMeta)) {
					msg := &GossipMessage{
						Message: &GossipMessage_Advertise{
							Advertise: &Advertise{
								Header: GossipHeader{
									Timestamp: replica.Clock().Scheme().Codec().EncodeTimestamp(replica.Clock().Increment()),
								},
								ObjectMeta: object.ObjectMeta,
								Key:        object.Key,
							},
						},
					}
					log.Debugf("Sending GossipMessage %s->%s %+v", localID, senderID, msg)
					err := stream.Send(msg)
					if err != nil {
						return err
					}
				}
			}
		case *GossipMessage_Update:
			replica.Clock().Update(time.NewTimestamp(m.Update.Header.Timestamp))
			err := replica.Update(stream.Context(), &m.Update.Object)
			if err != nil {
				return err
			}
		}
	}
}

func (s *GossipServer) Read(ctx context.Context, request *ReadRequest) (*ReadResponse, error) {
	member, _ := s.manager.Cluster.Member()
	log.Debugf("Received ReadRequest %s->%s %+v", request.Header.MemberID, member.ID, request)
	replica, err := s.getReplica(ctx, request.Header.PartitionID, request.Header.ServiceID, &request.Header.Timestamp)
	if err != nil {
		return nil, errors.Proto(err)
	}
	timestamp := replica.Clock().Update(time.NewTimestamp(request.Header.Timestamp))
	object, err := replica.Read(ctx, request.Key)
	if err != nil {
		return nil, errors.Proto(err)
	}
	response := &ReadResponse{
		Header: ResponseHeader{
			Timestamp: replica.Clock().Scheme().Codec().EncodeTimestamp(timestamp),
		},
		Object: object,
	}
	log.Debugf("Sending ReadResponse %s->%s %+v", member.ID, request.Header.MemberID, response)
	return response, nil
}

func (s *GossipServer) ReadAll(request *ReadAllRequest, stream GossipProtocol_ReadAllServer) error {
	member, _ := s.manager.Cluster.Member()
	log.Debugf("Received ReadAllRequest %s->%s %+v", request.Header.MemberID, member.ID, request)
	replica, err := s.getReplica(stream.Context(), request.Header.PartitionID, request.Header.ServiceID, &request.Header.Timestamp)
	if err != nil {
		return errors.Proto(err)
	}
	timestamp := replica.Clock().Update(time.NewTimestamp(request.Header.Timestamp))

	objectCh := make(chan Object)
	errCh := make(chan error)
	go func() {
		err := replica.ReadAll(stream.Context(), objectCh)
		if err != nil {
			errCh <- err
		}
	}()

	closed := false
	for {
		select {
		case object, ok := <-objectCh:
			if ok {
				response := &ReadAllResponse{
					Header: ResponseHeader{
						Timestamp: replica.Clock().Scheme().Codec().EncodeTimestamp(timestamp),
					},
					Object: object,
				}
				log.Debugf("Sending ReadAllResponse %s->%s %+v", member.ID, request.Header.MemberID, response)
				err := stream.Send(response)
				if err != nil {
					return errors.Proto(err)
				}
			} else if !closed {
				closed = true
			} else {
				return nil
			}
		case err := <-errCh:
			if err != nil {
				return err
			} else if !closed {
				closed = true
			} else {
				return nil
			}
		}
	}
}

func (s *GossipServer) getReplica(ctx context.Context, partitionID PartitionID, serviceID ServiceId, timestamp *metaapi.Timestamp) (Replica, error) {
	partition, err := s.manager.Partition(partitionID)
	if err != nil {
		return nil, err
	}
	return partition.getReplica(ctx, serviceID, timestamp)
}

func getClockFromTimestamp(timestamp metaapi.Timestamp) time.Clock {
	switch timestamp.Timestamp.(type) {
	case *metaapi.Timestamp_LogicalTimestamp:
		return time.NewLogicalClock()
	case *metaapi.Timestamp_PhysicalTimestamp:
		return time.NewPhysicalClock()
	}
	return time.NewLogicalClock()
}

func getReplicationFactorFromContext(ctx context.Context) int {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0
	}
	replicas := md.Get("Replication-Factor")
	if len(replicas) == 0 {
		return 0
	}
	i, err := strconv.Atoi(replicas[0])
	if err != nil {
		return 0
	}
	return i
}
