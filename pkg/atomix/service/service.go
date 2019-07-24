package service

import "time"

type OperationType string

const (
	OpTypeCommand OperationType = "command"
	OpTypeQuery   OperationType = "query"
)

// Service is an interface for primitive services
type Service interface {
	StateMachine

	// Backup must be implemented by services to return the serialized state of the service
	Backup() ([]byte, error)

	// Restore must be implemented by services to restore the state of the service from a serialized backup
	Restore(bytes []byte) error
}

// ServiceRegistry is a registry of service types
type ServiceRegistry struct {
	types map[string]func(ctx Context) Service
}

// Register registers a new primitive type
func (r *ServiceRegistry) Register(name string, f func(ctx Context) Service) {
	r.types[name] = f
}

// getType returns a service type by name
func (r *ServiceRegistry) getType(name string) func(sctx Context) Service {
	return r.types[name]
}

// NewServiceRegistry returns a new primitive type registry
func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{types: make(map[string]func(ctx Context) Service)}
}

// service is an internal base for service implementations
type service struct {
	Scheduler Scheduler
	Executor  Executor
	Context   Context
}

// NewResult returns a new result with the given output and error
func (s *service) NewResult(value []byte, err error) Result {
	return Result{
		Index: s.Context.Index(),
		Output: Output{
			Value: value,
			Error: err,
		},
	}
}

// NewSuccess returns a new successful result with the given output
func (s *service) NewSuccess(value []byte) Result {
	return Result{
		Index: s.Context.Index(),
		Output: Output{
			Value: value,
		},
	}
}

// NewFailure returns a new failure result with the given error
func (s *service) NewFailure(err error) Result {
	return Result{
		Index: s.Context.Index(),
		Output: Output{
			Error: err,
		},
	}
}

// mutableContext is an internal context implementation which supports per-service indexes
type mutableContext struct {
	Context
	index uint64
	time  time.Time
	op    OperationType
}

func (c *mutableContext) Index() uint64 {
	return c.index
}

func (c *mutableContext) Timestamp() time.Time {
	return c.time
}

func (c *mutableContext) OperationType() OperationType {
	return c.op
}

func (c *mutableContext) setCommand(time time.Time) {
	c.index = c.index + 1
	c.time = time
	c.op = OpTypeCommand
}

func (c *mutableContext) setQuery() {
	c.op = OpTypeQuery
}
