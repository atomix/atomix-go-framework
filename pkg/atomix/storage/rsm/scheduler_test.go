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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	sched := newScheduler()
	now := time.Now()
	sched.time = now

	run1 := 0
	sched.Execute(func() {
		run1++
	})

	run2 := 0
	timer2 := sched.ScheduleOnce(100*time.Millisecond, func() {
		run2++
	})

	run3 := 0
	_ = sched.ScheduleOnce(50*time.Millisecond, func() {
		run3++
	})

	run4 := 0
	timer4 := sched.ScheduleRepeat(50*time.Millisecond, 100*time.Millisecond, func() {
		run4++
	})

	sched.runImmediateTasks()
	assert.Equal(t, 1, run1)

	// 10 milliseconds later, no tasks should run
	sched.runScheduledTasks(now.Add(10 * time.Millisecond))

	assert.Equal(t, 0, run2)
	assert.Equal(t, 0, run3)
	assert.Equal(t, 0, run4)

	timer2.Cancel()

	// 75 milliseconds later, 3 and 4 should run
	sched.runScheduledTasks(now.Add(75 * time.Millisecond))

	assert.Equal(t, 0, run2)
	assert.Equal(t, 1, run3)
	assert.Equal(t, 1, run4)

	// 175 milliseconds later, 4 should have run twice
	sched.runScheduledTasks(now.Add(175 * time.Millisecond))

	assert.Equal(t, 0, run2)
	assert.Equal(t, 1, run3)
	assert.Equal(t, 2, run4)

	// 275 milliseconds later, 4 should have run 3 times
	sched.runScheduledTasks(now.Add(275 * time.Millisecond))

	assert.Equal(t, 3, run4)

	timer4.Cancel()

	sched.runScheduledTasks(now.Add(500 * time.Millisecond))

	assert.Equal(t, 1, run1)
	assert.Equal(t, 0, run2)
	assert.Equal(t, 1, run3)
	assert.Equal(t, 3, run4)
}
