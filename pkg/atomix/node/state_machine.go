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

package node

import (
	"encoding/binary"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
	"io"
	"strings"
	"time"
)

// Context provides information about the context within which a state machine is running
type Context interface {
	// Node is the local node identifier
	Node() string

	// Index returns the current index of the state machine
	Index() uint64

	// Timestamp returns a deterministic, monotonically increasing timestamp
	Timestamp() time.Time

	// OperationType returns the type of the operation currently being executed against the state machine
	OperationType() service.OperationType
}

// StateMachine applies commands from a protocol to a collection of state machines
type StateMachine interface {
	// Snapshot writes the state machine snapshot to the given writer
	Snapshot(writer io.Writer) error

	// Install reads the state machine snapshot from the given reader
	Install(reader io.Reader) error

	// CanDelete returns a bool indicating whether the node can delete changes up to the given index without affecting
	// the correctness of the state machine
	CanDelete(index uint64) bool

	// Command applies a command to the state machine
	Command(bytes []byte, ch chan<- Output)

	// Query applies a query to the state machine
	Query(bytes []byte, ch chan<- Output)
}

func newOutput(value []byte, err error) Output {
	return Output{
		Value: value,
		Error: err,
	}
}

func newFailure(err error) Output {
	return Output{
		Error: err,
	}
}

func fail(ch chan<- Output, err error) {
	if ch != nil {
		ch <- newFailure(err)
		close(ch)
	}
}

// Output is a state machine operation output
type Output struct {
	Value []byte
	Error error
}

// Failed returns a boolean indicating whether the operation failed
func (r Output) Failed() bool {
	return r.Error != nil
}

// Succeeded returns a boolean indicating whether the operation was successful
func (r Output) Succeeded() bool {
	return !r.Failed()
}

// NewPrimitiveStateMachine returns a new primitive state machine
func NewPrimitiveStateMachine(registry *Registry, ctx Context) StateMachine {
	return &primitiveStateMachine{
		ctx:      ctx,
		registry: registry,
		services: make(map[string]*serviceStateMachine),
	}
}

// primitiveStateMachine is the primary state machine for managing primitive services
type primitiveStateMachine struct {
	StateMachine
	ctx      Context
	registry *Registry
	services map[string]*serviceStateMachine
}

func (s *primitiveStateMachine) Snapshot(writer io.Writer) error {
	for id, svc := range s.services {
		serviceID := &service.ServiceId{
			Type:      svc.Type,
			Name:      getServiceName(id),
			Namespace: getServiceNamespace(id),
		}
		bytes, err := proto.Marshal(serviceID)
		if err != nil {
			return err
		}

		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(len(bytes)))

		_, err = writer.Write(length)
		if err != nil {
			return err
		}

		_, err = writer.Write(bytes)
		if err != nil {
			return err
		}

		err = svc.Snapshot(writer)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *primitiveStateMachine) Install(reader io.Reader) error {
	s.services = make(map[string]*serviceStateMachine)
	lengthBytes := make([]byte, 4)
	n, err := reader.Read(lengthBytes)
	if err != nil {
		return err
	}
	for n > 0 {
		length := binary.BigEndian.Uint32(lengthBytes)
		bytes := make([]byte, length)
		_, err = reader.Read(bytes)
		if err != nil {
			return err
		}

		serviceID := &service.ServiceId{}
		if err = proto.Unmarshal(bytes, serviceID); err != nil {
			return err
		}
		svc := s.registry.services[serviceID.Type](newServiceContext(s.ctx, serviceID))
		s.services[getQualifiedServiceName(serviceID)] = newServiceStateMachine(serviceID.Type, svc)

		n, err = reader.Read(lengthBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *primitiveStateMachine) Command(bytes []byte, ch chan<- Output) {
	request := &service.ServiceRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		fail(ch, err)
	} else {
		switch r := request.Request.(type) {
		case *service.ServiceRequest_Command:
			// If the service doesn't exist, create it.
			svc, ok := s.services[getQualifiedServiceName(request.Id)]
			if !ok {
				serviceType := s.registry.getType(request.Id.Type)
				if serviceType == nil {
					fail(ch, fmt.Errorf("unknown service type %s", request.Id.Type))
					return
				} else {
					svc = newServiceStateMachine(request.Id.Type, serviceType(newServiceContext(s.ctx, request.Id)))
					s.services[getQualifiedServiceName(request.Id)] = svc
				}
			}

			// Create a channel for the raw service results
			var serviceCh chan Output
			if ch != nil {
				serviceCh = make(chan Output)

				// Start a goroutine to encode the raw service results in a ServiceResponse
				go func() {
					defer close(ch)
					for result := range serviceCh {
						if result.Failed() {
							ch <- result
						} else {
							ch <- newOutput(proto.Marshal(&service.ServiceResponse{
								Response: &service.ServiceResponse_Command{
									Command: result.Value,
								},
							}))
						}
					}
				}()
			}

			// Execute the command on the service
			svc.Command(r.Command, serviceCh)
		case *service.ServiceRequest_Create:
			_, ok := s.services[getQualifiedServiceName(request.Id)]
			if !ok {
				serviceType := s.registry.getType(request.Id.Type)
				if serviceType == nil {
					fail(ch, fmt.Errorf("unknown service type %s", request.Id.Type))
				} else {
					svc := serviceType(newServiceContext(s.ctx, request.Id))
					s.services[getQualifiedServiceName(request.Id)] = newServiceStateMachine(request.Id.Type, svc)

					if ch != nil {
						ch <- newOutput(proto.Marshal(&service.ServiceResponse{
							Response: &service.ServiceResponse_Create{
								Create: &service.CreateResponse{},
							},
						}))
					}
				}
			} else {
				if ch != nil {
					ch <- newOutput(proto.Marshal(&service.ServiceResponse{
						Response: &service.ServiceResponse_Create{
							Create: &service.CreateResponse{},
						},
					}))
				}
			}
		case *service.ServiceRequest_Delete:
			delete(s.services, getQualifiedServiceName(request.Id))

			if ch != nil {
				ch <- newOutput(proto.Marshal(&service.ServiceResponse{
					Response: &service.ServiceResponse_Delete{
						Delete: &service.DeleteResponse{},
					},
				}))
			}
		}
	}
}

func (s *primitiveStateMachine) Query(bytes []byte, ch chan<- Output) {
	request := &service.ServiceRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		fail(ch, err)
	} else {
		switch r := request.Request.(type) {
		case *service.ServiceRequest_Query:
			svc, ok := s.services[getQualifiedServiceName(request.Id)]
			if !ok {
				fail(ch, fmt.Errorf("unknown service %s", getQualifiedServiceName(request.Id)))
			} else {
				// Create a channel for the raw service results
				var serviceCh chan Output
				if ch != nil {
					serviceCh = make(chan Output)

					// Start a goroutine to encode the raw service results in a ServiceResponse
					go func() {
						defer close(ch)
						for result := range serviceCh {
							if result.Failed() {
								ch <- result
							} else {
								ch <- newOutput(proto.Marshal(&service.ServiceResponse{
									Response: &service.ServiceResponse_Query{
										Query: result.Value,
									},
								}))
							}
						}
					}()
				}

				// Execute the query on the service
				svc.Query(r.Query, serviceCh)
			}
		case *service.ServiceRequest_Metadata:
			services := make([]*service.ServiceId, 0, len(s.services))
			for id, svc := range s.services {
				namespace := getServiceNamespace(id)
				if (r.Metadata.Namespace == "" || namespace == r.Metadata.Namespace) && (r.Metadata.Type == "" || svc.Type == r.Metadata.Type) {
					services = append(services, &service.ServiceId{
						Type:      svc.Type,
						Name:      getServiceName(id),
						Namespace: namespace,
					})
				}
			}

			if ch != nil {
				ch <- newOutput(proto.Marshal(&service.ServiceResponse{
					Response: &service.ServiceResponse_Metadata{
						Metadata: &service.MetadataResponse{
							Services: services,
						},
					},
				}))
				close(ch)
			}
		}
	}
}

func getQualifiedServiceName(id *service.ServiceId) string {
	return id.Namespace + "." + id.Name
}

func getServiceNamespace(id string) string {
	return strings.Split(id, ".")[0]
}

func getServiceName(id string) string {
	return strings.Split(id, ".")[1]
}

// newServiceStateMachine returns a new wrapped service
func newServiceStateMachine(serviceType string, service service.Service) *serviceStateMachine {
	return &serviceStateMachine{
		Type:    serviceType,
		service: service,
	}
}

// serviceStateMachine is a typed wrapper around a service
type serviceStateMachine struct {
	StateMachine
	Type    string
	service service.Service
}

func (s *serviceStateMachine) Snapshot(writer io.Writer) error {
	return s.service.Snapshot(writer)
}

func (s *serviceStateMachine) Install(reader io.Reader) error {
	return s.service.Install(reader)
}

func (s *serviceStateMachine) CanDelete(index uint64) bool {
	return s.service.CanDelete(index)
}

func (s *serviceStateMachine) Command(bytes []byte, ch chan<- Output) {
	resultCh := make(chan service.Result)
	go func() {
		for result := range resultCh {
			ch <- Output{
				Value: result.Value,
				Error: result.Error,
			}
		}
		defer func() {
			_ = recover()
		}()
		close(ch)
	}()
	s.service.Command(bytes, resultCh)
}

func (s *serviceStateMachine) Query(bytes []byte, ch chan<- Output) {
	resultCh := make(chan service.Result)
	go func() {
		for result := range resultCh {
			ch <- Output{
				Value: result.Value,
				Error: result.Error,
			}
		}
		defer func() {
			_ = recover()
		}()
		close(ch)
	}()
	s.service.Query(bytes, resultCh)
}

func newServiceContext(ctx Context, serviceID *service.ServiceId) service.Context {
	return &serviceContext{
		parent: ctx,
		id:     serviceID,
	}
}

// serviceContext is a minimal service.Context to provide metadata to services
type serviceContext struct {
	parent Context
	id     *service.ServiceId
}

func (c *serviceContext) Index() uint64 {
	return c.parent.Index()
}

func (c *serviceContext) Timestamp() time.Time {
	return c.parent.Timestamp()
}

func (c *serviceContext) OperationType() service.OperationType {
	return c.parent.OperationType()
}

func (c *serviceContext) Node() string {
	return c.parent.Node()
}

func (c *serviceContext) Namespace() string {
	return c.id.Namespace
}

func (c *serviceContext) Name() string {
	return c.id.Name
}
