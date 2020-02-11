// Copyright 2020-present Open Networking Foundation.
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

package log

import (
	"context"
	"testing"

	client "github.com/atomix/go-client/pkg/client/log"
	"github.com/atomix/go-client/pkg/client/primitive"
	_ "github.com/atomix/go-framework/pkg/atomix/session"
	"github.com/atomix/go-framework/pkg/atomix/test"
	"github.com/stretchr/testify/assert"
)

func TestLog(t *testing.T) {
	partition, node := test.StartTestNode()
	defer node.Stop()

	session, err := primitive.NewSession(context.TODO(), partition)
	assert.NoError(t, err)
	defer session.Close()

	// Creates a new log primitive
	name := primitive.NewName("default", "test", "default", "test")
	log, err := client.New(context.TODO(), name, []*primitive.Session{session})
	assert.NoError(t, err)

	// Gets the log entry at index 0
	kv, err := log.Get(context.Background(), 0)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	// Checks the size of log primitive
	size, err := log.Size(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	// Appends  an entry to the log
	kv, err = log.Append(context.Background(), []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	// Appends an entry to the log
	kv, err = log.Append(context.Background(), []byte("baz"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", string(kv.Value))

	// Gets the first entry in the log
	kv, err = log.FirstEntry(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(kv.Value))

	// Gets the first index
	firstIndex, err := log.FirstIndex(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x1), uint64(firstIndex))

	// Gets the last entry in the log
	kv, err = log.LastEntry(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(kv.Value))

	// Gets the last index
	lastIndex, err := log.LastIndex(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x2), uint64(lastIndex))

	// Gets the next entry of the given index in the log
	kv, err = log.NextEntry(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(kv.Value))

	// Gets the previous entry of the given index in the log
	kv, err = log.PrevEntry(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(kv.Value))

	// Gets the log entry at index 1
	kv, err = log.Get(context.Background(), 1)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	// Gets the size of the log primitive
	size, err = log.Size(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, size)

	// Removes the entry at index 1 from the log
	kv, err = log.Remove(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(kv.Value))

	// Removes the entry at index 2 from the log
	kv, err = log.Remove(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(kv.Value))

	// Checks the length of the log primitive
	size, err = log.Size(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	err = log.Clear(context.Background())
	assert.NoError(t, err)
}

func TestLogStreams(t *testing.T) {
	partition, node := test.StartTestNode()
	defer node.Stop()

	session, err := primitive.NewSession(context.TODO(), partition)
	assert.NoError(t, err)
	defer session.Close()

	// Creates a new log primitive
	name := primitive.NewName("default", "test", "default", "test")
	log, err := client.New(context.TODO(), name, []*primitive.Session{session})
	assert.NoError(t, err)

	kv, err := log.Append(context.Background(), []byte("item1"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	c := make(chan *client.Event)
	latch := make(chan struct{})
	go func() {
		e := <-c
		assert.Equal(t, "item2", string(e.Entry.Value))
		e = <-c
		assert.Equal(t, "item3", string(e.Entry.Value))
		e = <-c
		assert.Equal(t, "item4", string(e.Entry.Value))
		e = <-c
		assert.Equal(t, "item5", string(e.Entry.Value))
		latch <- struct{}{}
	}()

	err = log.Watch(context.Background(), c)
	assert.NoError(t, err)

	kv, err = log.Append(context.Background(), []byte("item2"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "item2", string(kv.Value))

	kv, err = log.Append(context.Background(), []byte("item3"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "item3", string(kv.Value))

	kv, err = log.Append(context.Background(), []byte("item4"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "item4", string(kv.Value))

	kv, err = log.Append(context.Background(), []byte("item5"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "item5", string(kv.Value))

	chanEntry := make(chan *client.Entry)
	go func() {
		e := <-chanEntry
		assert.Equal(t, "item1", string(e.Value))
		e = <-chanEntry
		assert.Equal(t, "item2", string(e.Value))
		e = <-chanEntry
		assert.Equal(t, "item3", string(e.Value))
		e = <-chanEntry
		assert.Equal(t, "item4", string(e.Value))
		e = <-chanEntry
		assert.Equal(t, "item5", string(e.Value))
		latch <- struct{}{}
	}()

	err = log.Entries(context.Background(), chanEntry)
	assert.NoError(t, err)

	err = log.Close(context.Background())
	assert.NoError(t, err)

	log1, err := client.New(context.TODO(), name, []*primitive.Session{session})
	assert.NoError(t, err)

	log2, err := client.New(context.TODO(), name, []*primitive.Session{session})
	assert.NoError(t, err)

	size, err := log1.Size(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 5, size)

	err = log1.Close(context.Background())
	assert.NoError(t, err)

	err = log1.Delete(context.Background())
	assert.NoError(t, err)

	err = log2.Delete(context.Background())
	assert.NoError(t, err)

	log, err = client.New(context.TODO(), name, []*primitive.Session{session})
	assert.NoError(t, err)

	size, err = log.Size(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)
}
