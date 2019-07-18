package service

import (
	"container/list"
	"time"
)

// Scheduler provides deterministic scheduling for a state machine
type Scheduler interface {
	// Execute executes a function asynchronously
	Execute(f func())

	// ScheduleOnce schedules a function to be run once after the given delay
	ScheduleOnce(delay time.Duration, f func())

	// ScheduleRepeat schedules a function to run repeatedly every interval starting after the given delay
	ScheduleRepeat(delay time.Duration, interval time.Duration, f func())

	// ScheduleIndex schedules a function to run at a specific index
	ScheduleIndex(index uint64, f func())
}

func newScheduler() *scheduler {
	return &scheduler{
		tasks:          list.New(),
		scheduledTasks: list.New(),
		indexTasks:     make(map[uint64]*list.List),
		time:           time.Now(),
	}
}

type scheduler struct {
	Scheduler
	tasks          *list.List
	scheduledTasks *list.List
	indexTasks     map[uint64]*list.List
	time           time.Time
}

func (s *scheduler) Execute(f func()) {
	s.tasks.PushBack(f)
}

func (s *scheduler) ScheduleOnce(delay time.Duration, f func()) {
	s.scheduledTasks.PushBack(&task{
		scheduler: s,
		time: time.Now().Add(delay),
		interval: 0,
		callback: f,
	})
}

func (s *scheduler) ScheduleRepeat(delay time.Duration, interval time.Duration, f func()) {
	s.scheduledTasks.PushBack(&task{
		scheduler: s,
		time: time.Now().Add(delay),
		interval: interval,
		callback: f,
	})
}

func (s *scheduler) ScheduleIndex(index uint64, f func()) {
	tasks, ok := s.indexTasks[index]
	if !ok {
		tasks = list.New()
		s.indexTasks[index] = tasks
	}
	tasks.PushBack(f)
}

// runImmediateTasks runs the immediate tasks in the scheduler queue
func (s *scheduler) runImmediateTasks() {
	task := s.tasks.Front()
	for task != nil {
		task.Value.(func())()
		task = task.Next()
	}
	s.tasks = list.New()
}

// runScheduleTasks runs the scheduled tasks in the scheduler queue
func (s *scheduler) runScheduledTasks(time time.Time) {
	s.time = time
	element := s.scheduledTasks.Front()
	if element != nil {
		complete := list.New()
		for element != nil {
			task := element.Value.(*task)
			if task.isRunnable(time) {
				s.time = task.time
				task.run()
				complete.PushBack(task)
				element = element.Next()
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

// runIndex runs functions pending at the given index
func (s *scheduler) runIndex(index uint64) {
	tasks, ok := s.indexTasks[index]
	if ok {
		task := tasks.Front()
		for task != nil {
			task.Value.(func())()
			task = task.Next()
		}
		delete(s.indexTasks, index)
	}
}

// schedule schedules a task
func (s *scheduler) schedule(t *task) {
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

// Scheduler task
type task struct {
	scheduler *scheduler
	interval time.Duration
	callback func()
	time time.Time
	element *list.Element
}

func (t *task) isRunnable(time time.Time) bool {
	return time.UnixNano() > t.time.UnixNano()
}

func (t *task) run() {
	t.callback()
}

func (t *task) cancel() {
	if t.element != nil {
		t.scheduler.scheduledTasks.Remove(t.element)
	}
}
