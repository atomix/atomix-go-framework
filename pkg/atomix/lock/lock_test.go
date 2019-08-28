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

package lock

import (
	"context"
	client "github.com/atomix/atomix-go-client/pkg/client/lock"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-node/pkg/atomix/test"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	node, conn := test.StartTestNode()
	defer node.Stop()

	name := primitive.NewName("default", "test", "default", "test")
	l1, err := client.New(context.TODO(), name, []*grpc.ClientConn{conn}, session.WithTimeout(5*time.Second))
	assert.NoError(t, err)
	l2, err := client.New(context.TODO(), name, []*grpc.ClientConn{conn}, session.WithTimeout(5*time.Second))
	assert.NoError(t, err)

	v1, err := l1.Lock(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, 0, v1)

	locked, err := l1.IsLocked(context.Background())
	assert.NoError(t, err)
	assert.True(t, locked)

	locked, err = l2.IsLocked(context.Background())
	assert.NoError(t, err)
	assert.True(t, locked)

	var v2 uint64
	c := make(chan struct{})
	go func() {
		v2, err = l2.Lock(context.Background())
		assert.NoError(t, err)
		c <- struct{}{}
	}()

	success, err := l1.Unlock(context.Background())
	assert.NoError(t, err)
	assert.True(t, success)

	<-c

	assert.NotEqual(t, v1, v2)

	locked, err = l1.IsLocked(context.Background())
	assert.NoError(t, err)
	assert.True(t, locked)

	locked, err = l1.IsLocked(context.Background(), client.IfVersion(v1))
	assert.NoError(t, err)
	assert.False(t, locked)

	locked, err = l1.IsLocked(context.Background(), client.IfVersion(v2))
	assert.NoError(t, err)
	assert.True(t, locked)

	v2, err = l2.Lock(context.Background(), client.WithTimeout(1*time.Second))
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), v2)

	err = l1.Close()
	assert.NoError(t, err)

	err = l1.Delete()
	assert.NoError(t, err)

	err = l2.Delete()
	assert.NoError(t, err)

	l, err := client.New(context.TODO(), name, []*grpc.ClientConn{conn}, session.WithTimeout(5*time.Second))
	assert.NoError(t, err)

	locked, err = l.IsLocked(context.TODO())
	assert.NoError(t, err)
	assert.False(t, locked)
}
