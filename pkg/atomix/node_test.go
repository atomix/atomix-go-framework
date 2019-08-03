package atomix

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-go-node/proto/atomix/controller"
	"github.com/atomix/atomix-go-node/proto/atomix/headers"
	"github.com/atomix/atomix-go-node/proto/atomix/list"
	"github.com/atomix/atomix-go-node/proto/atomix/lock"
	"github.com/atomix/atomix-go-node/proto/atomix/map"
	"github.com/atomix/atomix-go-node/proto/atomix/primitive"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io"
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

func (p *TestProtocol) Start(cluster Cluster, registry *service.ServiceRegistry) error {
	p.context = &TestContext{}
	p.stateMachine = service.NewPrimitiveStateMachine(registry, p.context)
	p.client = &TestClient{
		stateMachine: p.stateMachine,
		context:      p.context,
		ch:           make(chan testRequest),
	}
	p.client.start()
	return nil
}

func (p *TestProtocol) Client() service.Client {
	return p.client
}

func (p *TestProtocol) Stop() error {
	p.client.stop()
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
	stateMachine service.StateMachine
	context      *TestContext
	ch           chan testRequest
}

type testRequest struct {
	op    service.OperationType
	input []byte
	ch    chan<- service.Output
}

func (c *TestClient) start() {
	go c.processRequests()
}

func (c *TestClient) stop() {
	close(c.ch)
}

func (c *TestClient) processRequests() {
	for request := range c.ch {
		if request.op == service.OpTypeCommand {
			c.context.index++
			c.context.timestamp = time.Now()
			c.context.operation = service.OpTypeCommand
			c.stateMachine.Command(request.input, request.ch)
		} else {
			c.context.operation = service.OpTypeQuery
			c.stateMachine.Query(request.input, request.ch)
		}
	}
}

func (c *TestClient) Write(ctx context.Context, input []byte, ch chan<- service.Output) error {
	c.ch <- testRequest{
		op:    service.OpTypeCommand,
		input: input,
		ch:    ch,
	}
	return nil
}

func (c *TestClient) Read(ctx context.Context, input []byte, ch chan<- service.Output) error {
	c.ch <- testRequest{
		op:    service.OpTypeQuery,
		input: input,
		ch:    ch,
	}
	return nil
}

func TestNode(t *testing.T) {
	node := NewNode("foo", &controller.PartitionConfig{}, NewTestProtocol())
	go node.Start()
	defer node.Stop()
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.Dial(":5678", grpc.WithInsecure())
	assert.NoError(t, err)
	defer conn.Close()

	client := primitive.NewPrimitiveServiceClient(conn)
	response, err := client.GetPrimitives(context.Background(), &primitive.GetPrimitivesRequest{})
	assert.NoError(t, err)
	assert.Len(t, response.Primitives, 0)
}

func TestList(t *testing.T) {
	node := NewNode("foo", &controller.PartitionConfig{}, NewTestProtocol())
	go node.Start()
	defer node.Stop()
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.Dial(":5678", grpc.WithInsecure())
	assert.NoError(t, err)
	defer conn.Close()

	client := list.NewListServiceClient(conn)

	createResponse, err := client.Create(context.TODO(), &list.CreateRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
		},
		Timeout: &duration.Duration{
			Seconds: 5,
		},
	})
	assert.NoError(t, err)

	sessionID := createResponse.Header.SessionId
	index := createResponse.Header.Index

	sizeResponse, err := client.Size(context.TODO(), &list.SizeRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 0,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), sizeResponse.Size)
	index = sizeResponse.Header.Index

	containsResponse, err := client.Contains(context.TODO(), &list.ContainsRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 0,
		},
		Value: "foo",
	})
	assert.NoError(t, err)
	assert.False(t, containsResponse.Contains)
	index = containsResponse.Header.Index

	appendResponse, err := client.Append(context.TODO(), &list.AppendRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
		Value: "foo",
	})
	assert.NoError(t, err)
	assert.Equal(t, list.ResponseStatus_OK, appendResponse.Status)
	index = appendResponse.Header.Index

	containsResponse, err = client.Contains(context.TODO(), &list.ContainsRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
		Value: "foo",
	})
	assert.NoError(t, err)
	assert.True(t, containsResponse.Contains)
	index = containsResponse.Header.Index

	sizeResponse, err = client.Size(context.TODO(), &list.SizeRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(1), sizeResponse.Size)
	index = sizeResponse.Header.Index

	removeResponse, err := client.Remove(context.TODO(), &list.RemoveRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 2,
		},
		Index: 0,
	})
	assert.NoError(t, err)
	assert.Equal(t, list.ResponseStatus_OK, appendResponse.Status)
	index = removeResponse.Header.Index

	sizeResponse, err = client.Size(context.TODO(), &list.SizeRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 2,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), sizeResponse.Size)
	index = sizeResponse.Header.Index

	listener, err := client.Listen(context.TODO(), &list.EventRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 3,
		},
	})
	assert.NoError(t, err)

	eventCh := make(chan bool)
	go func() {
		for {
			response, err := listener.Recv()
			if err != nil {
				return
			}
			if response.Type == list.EventResponse_ADDED && response.Value == "bar" {
				eventCh <- true
			}
		}
	}()

	appendResponse, err = client.Append(context.TODO(), &list.AppendRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 4,
		},
		Value: "bar",
	})
	assert.NoError(t, err)
	assert.Equal(t, list.ResponseStatus_OK, appendResponse.Status)
	index = appendResponse.Header.Index

	added, ok := <-eventCh
	assert.True(t, ok)
	assert.True(t, added)

	appendResponse, err = client.Append(context.TODO(), &list.AppendRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 5,
		},
		Value: "baz",
	})
	assert.NoError(t, err)
	assert.Equal(t, list.ResponseStatus_OK, appendResponse.Status)
	index = appendResponse.Header.Index

	iter, err := client.Iterate(context.TODO(), &list.IterateRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 5,
		},
	})
	assert.NoError(t, err)

	i := 0
	for {
		response, err := iter.Recv()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)

		if i == 0 {
			assert.Equal(t, "bar", response.Value)
			i++
		} else if i == 1 {
			assert.Equal(t, "baz", response.Value)
			i++
		}
	}
	assert.Equal(t, 2, i)
}

func TestMap(t *testing.T) {
	node := NewNode("foo", &controller.PartitionConfig{}, NewTestProtocol())
	go node.Start()
	defer node.Stop()
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.Dial(":5678", grpc.WithInsecure())
	assert.NoError(t, err)
	defer conn.Close()

	client := _map.NewMapServiceClient(conn)

	createResponse, err := client.Create(context.TODO(), &_map.CreateRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
		},
		Timeout: &duration.Duration{
			Seconds: 5,
		},
	})
	assert.NoError(t, err)

	sessionID := createResponse.Header.SessionId
	index := createResponse.Header.Index

	sizeResponse, err := client.Size(context.TODO(), &_map.SizeRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 0,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), sizeResponse.Size)
	index = sizeResponse.Header.Index

	putResponse, err := client.Put(context.TODO(), &_map.PutRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
		Key:   "foo",
		Value: []byte("Hello world!"),
	})
	assert.NoError(t, err)
	assert.Equal(t, _map.ResponseStatus_OK, putResponse.Status)
	index = putResponse.Header.Index

	getResponse, err := client.Get(context.TODO(), &_map.GetRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
		Key: "foo",
	})
	assert.NoError(t, err)
	assert.Equal(t, "Hello world!", string(getResponse.Value))
	index = getResponse.Header.Index
	version := getResponse.Version

	putResponse, err = client.Put(context.TODO(), &_map.PutRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
		Key:   "foo",
		Value: []byte("Hello world!"),
	})
	assert.NoError(t, err)
	assert.Equal(t, _map.ResponseStatus_OK, putResponse.Status)
	index = putResponse.Header.Index

	getResponse, err = client.Get(context.TODO(), &_map.GetRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
		Key: "foo",
	})
	assert.NoError(t, err)
	assert.Equal(t, "Hello world!", string(getResponse.Value))
	index = getResponse.Header.Index
	assert.Equal(t, version, getResponse.Version)

	sizeResponse, err = client.Size(context.TODO(), &_map.SizeRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(1), sizeResponse.Size)
	index = sizeResponse.Header.Index

	removeResponse, err := client.Remove(context.TODO(), &_map.RemoveRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 2,
		},
		Key: "foo",
	})
	assert.NoError(t, err)
	assert.Equal(t, _map.ResponseStatus_OK, removeResponse.Status)
	assert.Equal(t, "Hello world!", string(removeResponse.PreviousValue))
	assert.Equal(t, version, removeResponse.PreviousVersion)
	index = removeResponse.Header.Index

	events, err := client.Events(context.TODO(), &_map.EventRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 3,
		},
	})
	assert.NoError(t, err)

	go func() {
		i := 0
		for {
			response, err := events.Recv()
			if err != nil {
				assert.Equal(t, 4, i)
				break
			}

			if i == 0 {
				assert.Equal(t, _map.EventResponse_INSERTED, response.Type)
				assert.Equal(t, "foo", response.Key)
				assert.Equal(t, "Hello world!", string(response.NewValue))
				i++
			} else if i == 1 {
				assert.Equal(t, _map.EventResponse_INSERTED, response.Type)
				assert.Equal(t, "bar", response.Key)
				assert.Equal(t, "Hello world again!", string(response.NewValue))
				i++
			} else if i == 2 {
				assert.Equal(t, _map.EventResponse_INSERTED, response.Type)
				assert.Equal(t, "baz", response.Key)
				assert.Equal(t, "Hello world again again!", string(response.NewValue))
				i++
			} else if i == 3 {
				assert.Equal(t, _map.EventResponse_REMOVED, response.Type)
				assert.Equal(t, "bar", response.Key)
				assert.Equal(t, "Hello world again!", string(response.OldValue))
				i++
			}
		}
	}()

	putResponse, err = client.Put(context.TODO(), &_map.PutRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 4,
		},
		Key:   "foo",
		Value: []byte("Hello world!"),
	})
	assert.NoError(t, err)
	assert.Equal(t, _map.ResponseStatus_OK, putResponse.Status)
	index = putResponse.Header.Index

	putResponse, err = client.Put(context.TODO(), &_map.PutRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 5,
		},
		Key:   "bar",
		Value: []byte("Hello world again!"),
	})
	assert.NoError(t, err)
	assert.Equal(t, _map.ResponseStatus_OK, putResponse.Status)
	index = putResponse.Header.Index
	version = int64(putResponse.Header.Index)

	putResponse, err = client.Put(context.TODO(), &_map.PutRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 6,
		},
		Key:   "baz",
		Value: []byte("Hello world again again!"),
	})
	assert.NoError(t, err)
	assert.Equal(t, _map.ResponseStatus_OK, putResponse.Status)
	index = putResponse.Header.Index

	removeResponse, err = client.Remove(context.TODO(), &_map.RemoveRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 7,
		},
		Key: "bar",
	})
	assert.NoError(t, err)
	assert.Equal(t, _map.ResponseStatus_OK, removeResponse.Status)
	assert.Equal(t, "Hello world again!", string(removeResponse.PreviousValue))
	assert.Equal(t, version, removeResponse.PreviousVersion)
	index = removeResponse.Header.Index

	entries, err := client.Entries(context.TODO(), &_map.EntriesRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 7,
		},
	})
	assert.NoError(t, err)

	foo := false
	bar := false
	received := 0
	for {
		response, err := entries.Recv()
		if err == io.EOF {
			assert.Equal(t, 2, received)
			break
		}
		assert.NoError(t, err)

		if response.Key == "foo" {
			assert.False(t, foo)
			assert.Equal(t, "Hello world!", string(response.Value))
			foo = true
			received++
		} else if response.Key == "baz" {
			assert.False(t, bar)
			assert.Equal(t, "Hello world again again!", string(response.Value))
			bar = true
			received++
		} else {
			assert.Fail(t, "unknown key")
		}
	}
}

func TestLock(t *testing.T) {
	node := NewNode("foo", &controller.PartitionConfig{}, NewTestProtocol())
	go node.Start()
	defer node.Stop()
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.Dial(":5678", grpc.WithInsecure())
	assert.NoError(t, err)
	defer conn.Close()

	client := lock.NewLockServiceClient(conn)

	createResponse, err := client.Create(context.TODO(), &lock.CreateRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
		},
		Timeout: &duration.Duration{
			Seconds: 5,
		},
	})
	assert.NoError(t, err)

	sessionID := createResponse.Header.SessionId
	index := createResponse.Header.Index

	lockResponse, err := client.Lock(context.TODO(), &lock.LockRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
		Timeout: &duration.Duration{
			Nanos: int32(-1),
		},
	})
	assert.NoError(t, err)
	assert.NotEqual(t, uint64(0), lockResponse.Version)
	version := lockResponse.Version
	index = lockResponse.Header.Index

	isLockedResponse, err := client.IsLocked(context.TODO(), &lock.IsLockedRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
	})
	assert.NoError(t, err)
	assert.True(t, isLockedResponse.IsLocked)

	isLockedResponse, err = client.IsLocked(context.TODO(), &lock.IsLockedRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
		Version: version,
	})
	assert.NoError(t, err)
	assert.True(t, isLockedResponse.IsLocked)

	isLockedResponse, err = client.IsLocked(context.TODO(), &lock.IsLockedRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 1,
		},
		Version: version + 1,
	})
	assert.NoError(t, err)
	assert.False(t, isLockedResponse.IsLocked)
	index = isLockedResponse.Header.Index

	unlockResponse, err := client.Unlock(context.TODO(), &lock.UnlockRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 2,
		},
		Version: version + 1,
	})
	assert.NoError(t, err)
	assert.False(t, unlockResponse.Unlocked)
	index = unlockResponse.Header.Index

	unlockResponse, err = client.Unlock(context.TODO(), &lock.UnlockRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId:      sessionID,
			Index:          index,
			SequenceNumber: 3,
		},
		Version: version,
	})
	assert.NoError(t, err)
	assert.True(t, unlockResponse.Unlocked)
	index = unlockResponse.Header.Index
}
