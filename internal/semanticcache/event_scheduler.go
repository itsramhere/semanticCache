package semanticcache

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

type FaultConfig struct {
	Seed                 int64   `json:"seed"`
	MaxDelayMS           int     `json:"max_delay_ms"`
	DuplicateProbability float64 `json:"duplicate_probability"`
	DropProbability      float64 `json:"drop_probability"`
}

func (c FaultConfig) Validate() error {
	if c.MaxDelayMS < 0 || c.MaxDelayMS > 86400000 || !finite(c.DuplicateProbability) || !finite(c.DropProbability) || c.DuplicateProbability < 0 || c.DuplicateProbability > 1 || c.DropProbability < 0 || c.DropProbability > 1 {
		return fmt.Errorf("invalid scheduler settings")
	}
	return nil
}

type ScheduledUpdate struct {
	Event      UpdateEvent `json:"event"`
	DeliveryAt time.Time   `json:"delivery_at"`
	Dropped    bool        `json:"dropped"`
	Duplicate  bool        `json:"duplicate"`
	Order      uint64      `json:"order"`
}

// EventScheduler uses an independent seeded RNG. Time advances explicitly;
// injection never sleeps and cannot affect projection/workload randomness.
type EventScheduler struct {
	config FaultConfig
	rng    *rand.Rand
	queue  []ScheduledUpdate
	order  uint64
}

func NewEventScheduler(c FaultConfig) (*EventScheduler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &EventScheduler{config: c, rng: rand.New(rand.NewSource(c.Seed))}, nil
}

func (s *EventScheduler) Schedule(e UpdateEvent) []ScheduledUpdate {
	item := func(duplicate bool) ScheduledUpdate {
		s.order++
		owned := e
		if e.Entry != nil {
			x := cloneEntry(*e.Entry)
			owned.Entry = &x
		}
		delay := s.rng.Intn(s.config.MaxDelayMS + 1)
		x := ScheduledUpdate{owned, e.Time.Add(time.Duration(delay) * time.Millisecond), false, duplicate, s.order}
		return x
	}
	first := item(false)
	first.Dropped = s.rng.Float64() < s.config.DropProbability
	result := []ScheduledUpdate{first}
	if first.Dropped {
		return result
	}
	// The queue owns its payload independently of returned scheduling evidence.
	enqueue := func(x ScheduledUpdate) {
		if x.Event.Entry != nil {
			entry := cloneEntry(*x.Event.Entry)
			x.Event.Entry = &entry
		}
		s.queue = append(s.queue, x)
	}
	enqueue(first)
	if s.rng.Float64() < s.config.DuplicateProbability {
		duplicate := item(true)
		result = append(result, duplicate)
		enqueue(duplicate)
	}
	return result
}

func (s *EventScheduler) Due(now time.Time) []ScheduledUpdate {
	sort.Slice(s.queue, func(i, j int) bool {
		a, b := s.queue[i], s.queue[j]
		if a.DeliveryAt.Equal(b.DeliveryAt) {
			return a.Order < b.Order
		}
		return a.DeliveryAt.Before(b.DeliveryAt)
	})
	i := 0
	for i < len(s.queue) && !s.queue[i].DeliveryAt.After(now) {
		i++
	}
	due := append([]ScheduledUpdate{}, s.queue[:i]...)
	s.queue = append([]ScheduledUpdate{}, s.queue[i:]...)
	return due
}

func (s *EventScheduler) Pending() int { return len(s.queue) }
