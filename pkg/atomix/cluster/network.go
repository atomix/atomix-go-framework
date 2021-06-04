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

package cluster

import (
	"context"
	"google.golang.org/grpc/test/bufconn"
	"io"
	"net"
	"sync"
)

// Network is an interface for creating net Conns and Listeners.
type Network interface {
	// Listen creates a new Listener
	Listen(address string) (net.Listener, error)
	// Connect creates a new Conn
	Connect(ctx context.Context, address string) (net.Conn, error)
}

// NewNetwork creates a new physical Network
func NewNetwork() Network {
	return &remoteNetwork{}
}

// remoteNetwork is a physical network
type remoteNetwork struct{}

func (n *remoteNetwork) Listen(address string) (net.Listener, error) {
	return net.Listen("tcp", address)
}

func (n *remoteNetwork) Connect(ctx context.Context, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", address)
}

const localBufSize = 1024 * 1024

// NewLocalNetwork creates a new process-local Network
func NewLocalNetwork() Network {
	return &localNetwork{
		listeners: make(map[string]*bufconn.Listener),
		watchers:  make(map[string][]chan<- *bufconn.Listener),
	}
}

type localNetwork struct {
	listeners   map[string]*bufconn.Listener
	listenersMu sync.Mutex
	watchers    map[string][]chan<- *bufconn.Listener
	watchersMu  sync.RWMutex
}

func (n *localNetwork) Listen(address string) (net.Listener, error) {
	lis := bufconn.Listen(localBufSize)
	n.listenersMu.Lock()
	n.listeners[address] = lis
	n.listenersMu.Unlock()

	n.watchersMu.RLock()
	watchers := n.watchers[address]
	delete(n.watchers, address)
	n.watchersMu.RUnlock()

	for _, watcher := range watchers {
		watcher <- lis
		close(watcher)
	}
	return lis, nil
}

func (n *localNetwork) Connect(ctx context.Context, address string) (net.Conn, error) {
	n.listenersMu.Lock()
	lis, ok := n.listeners[address]
	n.listenersMu.Unlock()
	if ok {
		return lis.Dial()
	}

	n.watchersMu.Lock()
	watcher := make(chan *bufconn.Listener)
	watchers := n.watchers[address]
	watchers = append(watchers, watcher)
	n.watchers[address] = watchers
	n.watchersMu.Unlock()

	select {
	case lis, ok := <-watcher:
		if !ok {
			return nil, io.EOF
		}
		return lis.Dial()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
