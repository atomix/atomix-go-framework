// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	"context"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
)

// newService creates a new Service for the given partition
// name is the name of the primitive
// handler is the primitive's session handler
func newService(session *Session, serviceInfo rsm.ServiceInfo) *Service {
	return &Service{
		Session:     session,
		serviceInfo: serviceInfo,
	}
}

// Service maintains the session for a primitive
type Service struct {
	*Session
	serviceInfo rsm.ServiceInfo
	serviceID   rsm.ServiceID
}

// DoCommand submits a command to the service
func (s *Service) DoCommand(ctx context.Context, operationID rsm.OperationID, input []byte) ([]byte, error) {
	return s.doCommand(ctx, s.serviceID, operationID, input)
}

// DoCommandStream submits a streaming command to the service
func (s *Service) DoCommandStream(ctx context.Context, operationID rsm.OperationID, input []byte, stream streams.WriteStream) error {
	return s.doCommandStream(ctx, s.serviceID, operationID, input, stream)
}

// DoQuery submits a query to the service
func (s *Service) DoQuery(ctx context.Context, operationID rsm.OperationID, input []byte, sync bool) ([]byte, error) {
	return s.doQuery(ctx, s.serviceID, operationID, input, sync)
}

// DoQueryStream submits a streaming query to the service
func (s *Service) DoQueryStream(ctx context.Context, operationID rsm.OperationID, input []byte, stream streams.WriteStream, sync bool) error {
	return s.doQueryStream(ctx, s.serviceID, operationID, input, stream, sync)
}

func (s *Service) open(ctx context.Context) error {
	request := &rsm.PartitionCommandRequest{
		PartitionID: rsm.PartitionID(s.partition.ID()),
		Request: rsm.CommandRequest{
			Request: &rsm.CommandRequest_SessionCommand{
				SessionCommand: &rsm.SessionCommandRequest{
					SessionID: s.sessionID,
					Request: &rsm.SessionCommandRequest_CreateService{
						CreateService: &rsm.CreateServiceRequest{
							ServiceInfo: s.serviceInfo,
						},
					},
				},
			},
		},
	}
	response, err := s.client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	s.serviceID = response.Response.GetSessionCommand().GetCreateService().ServiceID
	s.lastIndex.Update(response.Response.Index)
	return nil
}

func (s *Service) close(ctx context.Context) error {
	request := &rsm.PartitionCommandRequest{
		PartitionID: rsm.PartitionID(s.partition.ID()),
		Request: rsm.CommandRequest{
			Request: &rsm.CommandRequest_SessionCommand{
				SessionCommand: &rsm.SessionCommandRequest{
					SessionID: s.sessionID,
					Request: &rsm.SessionCommandRequest_CloseService{
						CloseService: &rsm.CloseServiceRequest{
							ServiceID: s.serviceID,
						},
					},
				},
			},
		},
	}
	response, err := s.client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	s.lastIndex.Update(response.Response.Index)
	s.servicesMu.Lock()
	delete(s.services, s.serviceInfo)
	s.servicesMu.Unlock()
	return nil
}
