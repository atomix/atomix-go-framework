package service

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	"io"
	"strings"
	"time"
)

type OperationType string

const (
	OpTypeCommand OperationType = "command"
	OpTypeQuery   OperationType = "query"
)

// Context provides information about the context within which a state machine is running
type Context interface {
	// Index returns the current index of the state machine
	Index() uint64

	// Timestamp returns a deterministic, monotonically increasing timestamp
	Timestamp() time.Time

	// OperationType returns the type of the operation currently being executed against the state machine
	OperationType() OperationType
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
	Command(bytes []byte, f func([]byte, error))

	// CommandStream applies a streaming command to the state machine
	CommandStream(bytes []byte, stream stream.Stream, f func(error))

	// Query applies a query to the state machine
	Query(bytes []byte, f func([]byte, error))

	// QueryStream applies a streaming query to the state machine
	QueryStream(bytes []byte, stream stream.Stream, f func(error))
}

// Service is an interface for primitive services
type Service interface {
	StateMachine
	Backup() ([]byte, error)
	Restore(bytes []byte) error
}

// Registry is a registry of service types
type Registry struct {
	types map[string]func(scheduler Scheduler, executor Executor, ctx Context) Service
}

// Register registers a new primitive type
func (r *Registry) Register(name string, f func(scheduler Scheduler, executor Executor, ctx Context) Service) {
	r.types[name] = f
}

// GetType returns a service type by name
func (r *Registry) GetType(name string) func(scheduler Scheduler, executor Executor, ctx Context) Service {
	return r.types[name]
}

// newRegistry returns a new primitive type registry
func newRegistry() *Registry {
	return &Registry{types: make(map[string]func(scheduler Scheduler, executor Executor, ctx Context) Service)}
}

// registry is the local service registry
var registry = newRegistry()

// newPrimitiveStateMachine returns a new primitive state machine
func NewPrimitiveStateMachine(ctx Context) StateMachine {
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
	for id, service := range s.services {
		serviceId := &ServiceId{
			Type:      service.Type,
			Name:      strings.Split(id, ":")[0],
			Namespace: strings.Split(id, ":")[1],
		}
		bytes, err := proto.Marshal(serviceId)
		if err != nil {
			return err
		} else {
			length := make([]byte, 4)
			binary.BigEndian.PutUint32(length, uint32(len(bytes)))
			writer.Write(length)
			writer.Write(bytes)
			service.Snapshot(writer)
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
		n, err = reader.Read(bytes)
		if err != nil {
			return err
		}

		serviceId := &ServiceId{}
		if err = proto.Unmarshal(bytes, serviceId); err != nil {
			return err
		}
		service := s.registry.types[serviceId.Type](newScheduler(), newExecutor(), s.ctx)
		s.services[getServiceName(serviceId)] = newServiceStateMachine(serviceId.Type, service)

		n, err = reader.Read(lengthBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *primitiveStateMachine) Command(bytes []byte, callback func([]byte, error)) {
	request := &ServiceRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		callback(nil, err)
	} else {
		switch r := request.Request.(type) {
		case *ServiceRequest_Command:
			service, ok := s.services[getServiceName(request.Id)]
			if !ok {
				callback(nil, errors.New(fmt.Sprintf("unknown service %s", getServiceName(request.Id))))
			} else {
				service.Command(r.Command, func(bytes []byte, err error) {
					if err != nil {
						callback(nil, err)
					} else {
						callback(proto.Marshal(&ServiceResponse{
							Response: &ServiceResponse_Command{
								Command: bytes,
							},
						}))
					}
				})
			}
		case *ServiceRequest_Create:
			_, ok := s.services[getServiceName(request.Id)]
			if !ok {
				serviceType := s.registry.GetType(request.Id.Type)
				if serviceType == nil {
					callback(nil, errors.New(fmt.Sprintf("unknown service type %s", request.Id.Type)))
				} else {
					service := serviceType(newScheduler(), newExecutor(), s.ctx)
					s.services[getServiceName(request.Id)] = newServiceStateMachine(request.Id.Type, service)
					callback(proto.Marshal(&ServiceResponse{
						Response: &ServiceResponse_Create{
							Create: &CreateResponse{},
						},
					}))
				}
			} else {
				callback(proto.Marshal(&ServiceResponse{
					Response: &ServiceResponse_Create{
						Create: &CreateResponse{},
					},
				}))
			}
		case *ServiceRequest_Delete:
			delete(s.services, getServiceName(request.Id))
			callback(proto.Marshal(&ServiceResponse{
				Response: &ServiceResponse_Delete{
					Delete: &DeleteResponse{},
				},
			}))
		}
	}
}

func getServiceName(id *ServiceId) string {
	return id.Name + "." + id.Namespace
}

func (s *primitiveStateMachine) CommandStream(bytes []byte, stream stream.Stream, callback func(error)) {
	request := &ServiceRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		callback(err)
	} else {
		service, ok := s.services[getServiceName(request.Id)]
		if !ok {
			callback(errors.New(fmt.Sprintf("unknown service %s", getServiceName(request.Id))))
		} else {
			service.CommandStream(request.GetCommand(), stream, callback)
		}
	}
}

func (s *primitiveStateMachine) Query(bytes []byte, callback func([]byte, error)) {
	request := &ServiceRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		callback(nil, err)
	} else {
		switch r := request.Request.(type) {
		case *ServiceRequest_Query:
			service, ok := s.services[getServiceName(request.Id)]
			if !ok {
				callback(nil, errors.New(fmt.Sprintf("unknown service %s", getServiceName(request.Id))))
			} else {
				service.Query(r.Query, func(bytes []byte, err error) {
					if err != nil {
						callback(nil, err)
					} else {
						callback(proto.Marshal(&ServiceResponse{
							Response: &ServiceResponse_Query{
								Query: bytes,
							},
						}))
					}
				})
			}
		case *ServiceRequest_Metadata:
			services := make([]*ServiceId, 0, len(s.services))
			for id, service := range s.services {
				if r.Metadata.Type == "" || service.Type == r.Metadata.Type {
					services = append(services, &ServiceId{
						Type:      service.Type,
						Name:      strings.Split(id, ":")[0],
						Namespace: strings.Split(id, ":")[1],
					})
				}
			}
			callback(proto.Marshal(&ServiceResponse{
				Response: &ServiceResponse_Metadata{
					Metadata: &MetadataResponse{
						Services: services,
					},
				},
			}))
		}
	}
}

func (s *primitiveStateMachine) QueryStream(bytes []byte, stream stream.Stream, callback func(error)) {
	request := &ServiceRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		callback(err)
	} else {
		service, ok := s.services[getServiceName(request.Id)]
		if !ok {
			callback(errors.New(fmt.Sprintf("unknown service %s", getServiceName(request.Id))))
		} else {
			service.QueryStream(request.GetQuery(), stream, callback)
		}
	}
}

// newServiceStateMachine returns a new wrapped service
func newServiceStateMachine(serviceType string, service Service) *serviceStateMachine {
	return &serviceStateMachine{
		Type:    serviceType,
		service: service,
	}
}

// serviceStateMachine is a typed wrapper around a service
type serviceStateMachine struct {
	StateMachine
	Type    string
	service Service
}

func (s *serviceStateMachine) Snapshot(writer io.Writer) {
	s.service.Snapshot(writer)
}

func (s *serviceStateMachine) Install(reader io.Reader) {
	s.service.Install(reader)
}

func (s *serviceStateMachine) CanDelete(index uint64) bool {
	return s.service.CanDelete(index)
}

func (s *serviceStateMachine) Command(bytes []byte, callback func([]byte, error)) {
	s.service.Command(bytes, callback)
}

func (s *serviceStateMachine) Query(bytes []byte, callback func([]byte, error)) {
	s.service.Query(bytes, callback)
}
