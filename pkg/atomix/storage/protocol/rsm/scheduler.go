// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	"container/list"
	"time"
)

// Scheduler provides deterministic scheduling for a state machine
type Scheduler interface {
	// Time returns the current time
	Time() time.Time

	// Run executes a function asynchronously
	Run(f func())

	// RunAfter schedules a function to be run once after the given delay
	RunAfter(d time.Duration, f func()) Timer

	// RepeatAfter schedules a function to run repeatedly every interval starting after the given delay
	RepeatAfter(d time.Duration, i time.Duration, f func()) Timer

	// RunAt schedules a function to be run once after the given delay
	RunAt(t time.Time, f func()) Timer

	// RepeatAt schedules a function to run repeatedly every interval starting after the given delay
	RepeatAt(t time.Time, i time.Duration, f func()) Timer

	runImmediateTasks()
	runScheduledTasks(t time.Time)
}

// Timer is a cancellable timer
type Timer interface {
	// Cancel cancels the timer, preventing it from running in the future
	Cancel()
}

func newScheduler() *serviceScheduler {
	return &serviceScheduler{
		tasks:          list.New(),
		scheduledTasks: list.New(),
		time:           time.Now(),
	}
}

type serviceScheduler struct {
	tasks          *list.List
	scheduledTasks *list.List
	time           time.Time
}

func (s *serviceScheduler) Time() time.Time {
	return s.time
}

func (s *serviceScheduler) Run(f func()) {
	s.tasks.PushBack(f)
}

func (s *serviceScheduler) RunAfter(d time.Duration, f func()) Timer {
	task := &task{
		scheduler: s,
		time:      time.Now().Add(d),
		interval:  0,
		callback:  f,
	}
	s.schedule(task)
	return task
}

func (s *serviceScheduler) RepeatAfter(d time.Duration, i time.Duration, f func()) Timer {
	task := &task{
		scheduler: s,
		time:      time.Now().Add(d),
		interval:  i,
		callback:  f,
	}
	s.schedule(task)
	return task
}

func (s *serviceScheduler) RunAt(t time.Time, f func()) Timer {
	task := &task{
		scheduler: s,
		time:      t,
		interval:  0,
		callback:  f,
	}
	s.schedule(task)
	return task
}

func (s *serviceScheduler) RepeatAt(t time.Time, i time.Duration, f func()) Timer {
	task := &task{
		scheduler: s,
		time:      t,
		interval:  i,
		callback:  f,
	}
	s.schedule(task)
	return task
}

// runImmediateTasks runs the immediate tasks in the scheduler queue
func (s *serviceScheduler) runImmediateTasks() {
	task := s.tasks.Front()
	for task != nil {
		task.Value.(func())()
		task = task.Next()
	}
	s.tasks = list.New()
}

// runScheduleTasks runs the scheduled tasks in the scheduler queue
func (s *serviceScheduler) runScheduledTasks(time time.Time) {
	s.time = time
	element := s.scheduledTasks.Front()
	if element != nil {
		complete := list.New()
		for element != nil {
			task := element.Value.(*task)
			if task.isRunnable(time) {
				next := element.Next()
				s.scheduledTasks.Remove(element)
				s.time = task.time
				task.run()
				complete.PushBack(task)
				element = next
			} else {
				break
			}
		}

		element = complete.Front()
		for element != nil {
			task := element.Value.(*task)
			if task.interval > 0 {
				task.time = s.time.Add(task.interval)
				s.schedule(task)
			}
			element = element.Next()
		}
	}
}

// schedule schedules a task
func (s *serviceScheduler) schedule(t *task) {
	if s.scheduledTasks.Len() == 0 {
		t.element = s.scheduledTasks.PushBack(t)
	} else {
		element := s.scheduledTasks.Back()
		for element != nil {
			time := element.Value.(*task).time
			if element.Value.(*task).time.UnixNano() < time.UnixNano() {
				t.element = s.scheduledTasks.InsertAfter(t, element)
				return
			}
			element = element.Prev()
		}
		t.element = s.scheduledTasks.PushFront(t)
	}
}

var _ Scheduler = &serviceScheduler{}

// Scheduler task
type task struct {
	Timer
	scheduler *serviceScheduler
	interval  time.Duration
	callback  func()
	time      time.Time
	element   *list.Element
}

func (t *task) isRunnable(time time.Time) bool {
	return time.UnixNano() > t.time.UnixNano()
}

func (t *task) run() {
	t.callback()
}

func (t *task) Cancel() {
	if t.element != nil {
		t.scheduler.scheduledTasks.Remove(t.element)
	}
}
