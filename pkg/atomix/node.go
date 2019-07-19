package atomix

import (
	"fmt"
	map_ "github.com/atomix/atomix-go-node/pkg/atomix/map"
	"github.com/atomix/atomix-go-node/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-go-node/proto/atomix/controller"
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
func NewNode(nodeID string, config controller.PartitionConfig, protocol Protocol, opts ...NodeOption) *Node {
	node := &Node{
		Id:       nodeID,
		config:   config,
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
	node.listener = tcpListener{}
}

// withLocal sets the node to local mode for testing
func withLocal(lis net.Listener) NodeOption {
	return &localOption{lis}
}

type localOption struct {
	listener net.Listener
}

func (o *localOption) apply(node *Node) {
	node.listener = localListener{o.listener}
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
	Id       string
	config   controller.PartitionConfig
	protocol Protocol
	port     int
	listener listener
	server   *grpc.Server
}

// Start starts the node
func (n *Node) Start() error {
	if err := n.protocol.Start(getServiceRegistry()); err != nil {
		return err
	}

	lis, err := n.listener.listen(n)
	if err != nil {
		return err
	}

	n.server = grpc.NewServer()
	registerServers(n.server, n.protocol)
	return n.server.Serve(lis)
}

// Stop stops the node
func (n *Node) Stop() error {
	n.server.Stop()
	if err := n.protocol.Stop(); err != nil {
		return err
	}
	return nil
}

type listener interface {
	listen(*Node) (net.Listener, error)
}

type tcpListener struct{}

func (l tcpListener) listen(node *Node) (net.Listener, error) {
	return net.Listen("tcp", fmt.Sprintf(":%d", node.port))
}

type localListener struct {
	listener net.Listener
}

func (l localListener) listen(node *Node) (net.Listener, error) {
	return l.listener, nil
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
