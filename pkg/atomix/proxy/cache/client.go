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

package cache

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"google.golang.org/grpc"
	"math/rand"
	"sync"
)

// NewClient creates a new proxy client
func NewClient(cluster cluster.Cluster) *Client {
	return &Client{
		Cluster: cluster,
	}
}

// Client is a client for communicating with the storage layer
type Client struct {
	Cluster cluster.Cluster
	conn    *grpc.ClientConn
	mu      sync.RWMutex
}

func (c *Client) Connect() (*grpc.ClientConn, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	conn = c.conn
	if conn != nil {
		return conn, nil
	}

	replicas := make([]*cluster.Replica, 0)
	for _, replica := range c.Cluster.Replicas() {
		replicas = append(replicas, replica)
	}

	replica := replicas[rand.Intn(len(replicas))]
	conn, err := replica.Connect(context.Background(), cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

// close closes the connections
func (c *Client) Close() error {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}
