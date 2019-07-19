package atomix

import (
	"fmt"
	map_ "github.com/atomix/atomix-go-node/pkg/atomix/map"
	"github.com/atomix/atomix-go-node/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"google.golang.org/grpc"
	"net"
)

// Protocol
type Protocol interface {
	// Start starts the protocol
	Start(registry *service.ServiceRegistry) error

	// Client returns the protocol client
	Client() service.Client

	// Stop stops the protocol
	Stop() error
}

// NewNode creates a new node running the given protocol
func NewNode(protocol Protocol, opts ...NodeOption) *Node {
	node := &Node{
		protocol: protocol,
	}
	(&defaultOption{}).apply(node)
	for _, opt := range opts {
		opt.apply(node)
	}
	return node
}

// NodeOption is an option for constructing a Node
type NodeOption interface {
	apply(*Node)
}

// defaultOption is a node option which applies initial defaults
type defaultOption struct{}

func (o *defaultOption) apply(node *Node) {
	node.port = 5678
}

// WithPort sets the port on the node
func WithPort(port int) NodeOption {
	return &portOption{port: port}
}

type portOption struct {
	port int
}

func (o *portOption) apply(node *Node) {
	node.port = o.port
}

// Atomix node
type Node struct {
	protocol Protocol
	port     int
}

// Start starts the node
func (n *Node) Start() error {
	if err := n.protocol.Start(getServiceRegistry()); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", n.port))
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	registerServers(server, n.protocol)
	return server.Serve(lis)
}

// Stop stops the node
func (n *Node) Stop() error {
	if err := n.protocol.Stop(); err != nil {
		return err
	}
	return nil
}

// registerServers registers all primitive servers on the given gRPC server
func registerServers(server *grpc.Server, protocol Protocol) {
	primitive.RegisterPrimitiveServer(server, protocol.Client())
	map_.RegisterMapServer(server, protocol.Client())
}

// getServiceRegistry returns a service registry for the node
func getServiceRegistry() *service.ServiceRegistry {
	registry := service.NewServiceRegistry()
	map_.RegisterMapService(registry)
	return registry
}
