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
	"container/list"
	"github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &lockService{
		Service: rsm.NewService(scheduler, context),
		queue:   list.New(),
		timers:  make(map[rsm.Index]rsm.Timer),
	}
}

// lockService is a state machine for a list primitive
type lockService struct {
	rsm.Service
	lock   *lock.Lock
	queue  *list.List
	timers map[rsm.Index]rsm.Timer
}

func (l *lockService) Lock(input *lock.LockInput) (*LockOutputFuture, error) {
	panic("implement me")
}

func (l *lockService) Unlock(input *lock.UnlockInput) (*lock.UnlockOutput, error) {
	panic("implement me")
}

func (l *lockService) IsLocked(input *lock.IsLockedInput) (*lock.IsLockedOutput, error) {
	panic("implement me")
}

func (l *lockService) Snapshot() (*lock.Snapshot, error) {
	panic("implement me")
}

func (l *lockService) Restore(snapshot *lock.Snapshot) error {
	panic("implement me")
}
