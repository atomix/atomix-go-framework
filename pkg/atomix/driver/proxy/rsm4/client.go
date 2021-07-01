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

package rsm

import (
	"context"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	rsm "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm4"
	"google.golang.org/grpc"
	"sync"
	"time"
)

// ClientOption implements a client option
type ClientOption interface {
	prepare(options *clientOptions)
}

// WithClientTimeout returns a ClientOption to configure the session timeout
func WithClientTimeout(timeout time.Duration) ClientOption {
	return clientTimeoutOption{timeout: timeout}
}

type clientTimeoutOption struct {
	timeout time.Duration
}

func (o clientTimeoutOption) prepare(options *clientOptions) {
	options.timeout = o.timeout
}

type clientOptions struct {
	timeout time.Duration
	retry   time.Duration
}

// NewClient creates a new Client for the given partition
func NewClient(partition cluster.Partition, opts ...ClientOption) *Client {
	options := &clientOptions{
		retry:   15 * time.Second,
		timeout: time.Minute,
	}
	for i := range opts {
		opts[i].prepare(options)
	}
	return &Client{
		partition:     partition,
		Timeout:       options.timeout,
		retryInterval: options.retry,
		ticker:        time.NewTicker(options.timeout / 4),
		sessions:      make(map[rsm.SessionID]*Session),
	}
}

// Client is a client for communicating with the storage layer
type Client struct {
	partition     cluster.Partition
	Timeout       time.Duration
	clientID      rsm.ClientID
	retryInterval time.Duration
	conn          *grpc.ClientConn
	ticker        *time.Ticker
	sessions      map[rsm.SessionID]*Session
	mu            sync.RWMutex
}

func (c *Client) open(ctx context.Context) error {
	client := rsm.NewPartitionServiceClient(c.conn)
	request := &rsm.PartitionCommandRequest{
		Request: rsm.StateMachineCommandRequest{
			Request: &rsm.StateMachineCommandRequest_ClientConnect{
				ClientConnect: &rsm.ClientConnectRequest{
					SessionTimeout: c.Timeout,
				},
			},
		},
	}
	response, err := client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	c.clientID = response.Response.GetClientConnect().ClientID
	go func() {
		for range c.ticker.C {
			go c.keepAlive(context.Background())
		}
	}()
	return nil
}

func (c *Client) getState() rsm.ClientState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sessionStates := make([]rsm.SessionState, 0, len(c.sessions))
	for _, session := range c.sessions {
		sessionStates = append(sessionStates, session.getState())
	}
	return rsm.ClientState{
		ClientID: c.clientID,
		Sessions: sessionStates,
	}
}

func (c *Client) keepAlive(ctx context.Context) error {
	request := &rsm.PartitionCommandRequest{
		Request: rsm.StateMachineCommandRequest{
			Request: &rsm.StateMachineCommandRequest_ClientKeepAlive{
				ClientKeepAlive: &rsm.ClientKeepAliveRequest{
					State: c.getState(),
				},
			},
		},
	}
	client := rsm.NewPartitionServiceClient(c.conn)
	_, err := client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (c *Client) close(ctx context.Context) error {
	client := rsm.NewPartitionServiceClient(c.conn)
	request := &rsm.PartitionCommandRequest{
		Request: rsm.StateMachineCommandRequest{
			Request: &rsm.StateMachineCommandRequest_ClientClose{
				ClientClose: &rsm.ClientCloseRequest{
					ClientID: c.clientID,
				},
			},
		},
	}
	_, err := client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}
