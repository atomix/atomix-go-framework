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
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"io"
)

type GossipServer struct {
	manager *Manager
}

func (s *GossipServer) Gossip(stream GossipProtocol_GossipServer) error {
	msg, err := stream.Recv()
	if err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}

	init := msg.GetInitialize()
	replica, err := s.getReplica(stream.Context(), init.Header.PartitionID, init.Header.ServiceType, init.Header.ServiceID)
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

		switch m := msg.Message.(type) {
		case *GossipMessage_Advertise:
			timestamp := s.manager.clock.Update(time.NewTimestamp(m.Advertise.Header.Timestamp))
			object, err := replica.Read(stream.Context(), m.Advertise.Key)
			if err != nil {
				return err
			} else if object != nil {
				if meta.FromProto(object.ObjectMeta).After(meta.FromProto(m.Advertise.ObjectMeta)) {
					err := stream.Send(&GossipMessage{
						Message: &GossipMessage_Update{
							Update: &Update{
								Header: GossipHeader{
									Timestamp: s.manager.clock.Scheme().Codec().EncodeProto(timestamp),
								},
								Object: *object,
							},
						},
					})
					if err != nil {
						return err
					}
				} else if meta.FromProto(m.Advertise.ObjectMeta).After(meta.FromProto(object.ObjectMeta)) {
					err := stream.Send(&GossipMessage{
						Message: &GossipMessage_Advertise{
							Advertise: &Advertise{
								Header: GossipHeader{
									Timestamp: s.manager.clock.Scheme().Codec().EncodeProto(timestamp),
								},
								ObjectMeta: object.ObjectMeta,
								Key:        object.Key,
							},
						},
					})
					if err != nil {
						return err
					}
				}
			}
		case *GossipMessage_Update:
			s.manager.clock.Update(time.NewTimestamp(m.Update.Header.Timestamp))
			err := replica.Update(stream.Context(), &m.Update.Object)
			if err != nil {
				return err
			}
		}
	}
}

func (s *GossipServer) Read(ctx context.Context, request *ReadRequest) (*ReadResponse, error) {
	timestamp := s.manager.clock.Update(time.NewTimestamp(request.Header.Timestamp))
	replica, err := s.getReplica(ctx, request.Header.PartitionID, request.Header.ServiceType, request.Header.ServiceID)
	if err != nil {
		return nil, errors.Proto(err)
	}
	object, err := replica.Read(ctx, request.Key)
	if err != nil {
		return nil, errors.Proto(err)
	}
	return &ReadResponse{
		Header: ResponseHeader{
			Timestamp: s.manager.clock.Scheme().Codec().EncodeProto(timestamp),
		},
		Object: object,
	}, nil
}

func (s *GossipServer) ReadAll(request *ReadAllRequest, stream GossipProtocol_ReadAllServer) error {
	timestamp := s.manager.clock.Update(time.NewTimestamp(request.Header.Timestamp))
	replica, err := s.getReplica(stream.Context(), request.Header.PartitionID, request.Header.ServiceType, request.Header.ServiceID)
	if err != nil {
		return errors.Proto(err)
	}

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
				err := stream.Send(&ReadAllResponse{
					Header: ResponseHeader{
						Timestamp: s.manager.clock.Scheme().Codec().EncodeProto(timestamp),
					},
					Object: object,
				})
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

func (s *GossipServer) getReplica(ctx context.Context, partitionID PartitionID, serviceType ServiceType, serviceID ServiceID) (Replica, error) {
	partition, err := s.manager.Partition(partitionID)
	if err != nil {
		return nil, err
	}
	return partition.getReplica(ctx, serviceType, serviceID)
}
