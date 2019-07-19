package service

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
	types map[string]func(scheduler Scheduler, executor Executor, ctx Context) Service
}

// Register registers a new primitive type
func (r *ServiceRegistry) Register(name string, f func(scheduler Scheduler, executor Executor, ctx Context) Service) {
	r.types[name] = f
}

// getType returns a service type by name
func (r *ServiceRegistry) getType(name string) func(scheduler Scheduler, executor Executor, ctx Context) Service {
	return r.types[name]
}

// NewServiceRegistry returns a new primitive type registry
func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{types: make(map[string]func(scheduler Scheduler, executor Executor, ctx Context) Service)}
}
