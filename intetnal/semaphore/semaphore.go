package semaphore

import "sync"

type Semaphore struct {
	limit uint
	max   uint
	cond  *sync.Cond
}

func New(limit uint) *Semaphore {
	return &Semaphore{
		limit: limit,
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

func (s *Semaphore) Acquire() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	for s.max >= s.limit {
		s.cond.Wait()
	}

	s.max++
}

func (s *Semaphore) Release() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	s.max--
	s.cond.Signal()
}
