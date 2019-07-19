package atomix

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-go-node/proto/atomix/primitive"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"testing"
	"time"
)

func NewTestProtocol() Protocol {
	return &TestProtocol{}
}

type TestProtocol struct {
	Protocol
	stateMachine service.StateMachine
	client       *TestClient
	context      *TestContext
}

func (p *TestProtocol) Start(registry *service.ServiceRegistry) error {
	p.context = &TestContext{}
	p.stateMachine = service.NewPrimitiveStateMachine(registry, p.context)
	p.client = &TestClient{
		stateMachine: p.stateMachine,
		context:      p.context,
	}
	return nil
}

func (p *TestProtocol) Client() service.Client {
	return p.client
}

func (p *TestProtocol) Stop() error {
	return nil
}

type TestContext struct {
	service.Context
	index     uint64
	timestamp time.Time
	operation service.OperationType
}

func (c *TestContext) Index() uint64 {
	return c.index
}

func (c *TestContext) Timestamp() time.Time {
	return c.timestamp
}

func (c *TestContext) OperationType() service.OperationType {
	return c.operation
}

type TestClient struct {
	service.Client
	stateMachine service.StateMachine
	context      *TestContext
}

func (c *TestClient) Write(bytes []byte) ([]byte, error) {
	result := make(chan []byte, 1)
	errResult := make(chan error, 1)
	c.context.index++
	c.context.timestamp = time.Now()
	c.context.operation = service.OpTypeCommand
	c.stateMachine.Command(bytes, func(bytes []byte, err error) {
		if err != nil {
			errResult <- err
		} else {
			result <- bytes
		}
	})

	select {
	case r := <-result:
		return r, nil
	case e := <-errResult:
		return nil, e
	}
}

func (c *TestClient) WriteStream(bytes []byte, stream service.Stream) (error) {
	errResult := make(chan error, 1)
	c.context.index++
	c.context.timestamp = time.Now()
	c.context.operation = service.OpTypeCommand
	c.stateMachine.CommandStream(bytes, stream, func(err error) {
		errResult <- err
	})

	select {
	case e := <-errResult:
		return e
	}
}

func (c *TestClient) Read(bytes []byte) ([]byte, error) {
	result := make(chan []byte, 1)
	errResult := make(chan error, 1)
	c.context.operation = service.OpTypeQuery
	c.stateMachine.Query(bytes, func(bytes []byte, err error) {
		if err != nil {
			errResult <- err
		} else {
			result <- bytes
		}
	})

	select {
	case r := <-result:
		return r, nil
	case e := <-errResult:
		return nil, e
	}
}

func (c *TestClient) ReadStream(bytes []byte, stream service.Stream) (error) {
	errResult := make(chan error, 1)
	c.context.operation = service.OpTypeQuery
	c.stateMachine.QueryStream(bytes, stream, func(err error) {
		errResult <- err
	})

	select {
	case e := <-errResult:
		return e
	}
}

func TestNode(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	node := NewNode(NewTestProtocol(), withLocal(lis))
	go node.Start()

	f := func(c context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.Dial("test", grpc.WithContextDialer(f), grpc.WithInsecure())
	assert.NoError(t, err)

	client := primitive.NewPrimitiveServiceClient(conn)
	response, err := client.GetPrimitives(context.Background(), &primitive.GetPrimitivesRequest{})
	assert.NoError(t, err)
	assert.Len(t, response.Primitives, 0)

	node.Stop()
}
