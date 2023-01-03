package sked

import (
	"context"
	"errors"
	"time"
)

const (
	maxBackoffCoefficient     = 1 << 6
	defaultMessageBufferSize  = 1 << 10
	defaultRefreshInterval    = time.Second
	defaultMaxRefreshMessages = 1 << 16
)

var ErrCannotScheduleForPastDate = errors.New("cannot schedule for past date")

type Store interface {
	Put(context.Context, SavedMessage) (id string, err error)
	Capture(ctx context.Context, messages []SavedMessage, beforeThisTime time.Time) (n int, lockID string, err error)
	Delete(ctx context.Context, id string) error
	DeleteLocked(ctx context.Context, lockID string) error
}

type SavedMessage struct {
	LockID       string
	ID           string
	Payload      any
	ScheduleTime time.Time
	ScheduledAt  time.Time
}

type Message[T any] struct {
	Payload     T
	ScheduledAt time.Time
}

type Options struct {
	MessagesBufferSize int
	RefreshInterval    time.Duration
	MaxRefreshMessages int
	Errors             chan error
}

func NewSked[T any](ctx context.Context, store Store, opts Options) Sked[T] {
	if opts.MessagesBufferSize <= 0 {
		opts.MessagesBufferSize = defaultMessageBufferSize
	}

	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = defaultRefreshInterval
	}

	if opts.MaxRefreshMessages <= 0 {
		opts.MaxRefreshMessages = defaultMaxRefreshMessages
	}

	ctx, cancel := context.WithCancel(ctx)
	s := Sked[T]{
		store:              store,
		msgs:               make(chan Message[T], opts.MessagesBufferSize),
		refreshInterval:    opts.RefreshInterval,
		maxRefreshMessages: opts.MaxRefreshMessages,
		ctx:                ctx,
		cancel:             cancel,
		errChan:            opts.Errors,
	}

	go s.runRefresh()

	return s
}

type Sked[T any] struct {
	store              Store
	msgs               chan Message[T]
	errChan            chan error
	refreshInterval    time.Duration
	maxRefreshMessages int
	ctx                context.Context
	cancel             context.CancelFunc
}

func (s Sked[T]) Schedule(t time.Time, payload T) (int, error) {
	now := time.Now()
	if t.Before(now) {
		return 0, ErrCannotScheduleForPastDate
	}

	_, err := s.store.Put(s.ctx, SavedMessage{
		Payload:      payload,
		ScheduleTime: t,
		ScheduledAt:  now,
	})

	return 0, err
}

func (s Sked[T]) Messages() <-chan Message[T] {
	return s.msgs
}

func (s Sked[T]) Cancel() {
	s.cancel()
}

func (s Sked[T]) runRefresh() {
	backoffCoefficient := time.Duration(1)
	ticker := time.NewTicker(s.refreshInterval)
	savedMessages := make([]SavedMessage, s.maxRefreshMessages)
	for {
		select {
		case <-s.ctx.Done():
			ticker.Stop()
			s.sendErr(s.ctx.Err())
			return
		default:
		}

		n, _, err := s.store.Capture(s.ctx, savedMessages, time.Now().Add(s.refreshInterval))
		if err != nil {
			s.sendErr(err)
			// to prevent tight loop
			time.Sleep(backoffCoefficient * 100 * time.Millisecond)
			if backoffCoefficient < maxBackoffCoefficient {
				backoffCoefficient <<= 1
			}

			continue
		}
		backoffCoefficient = 1

		now := time.Now()
		for i := 0; i < n; i++ {
			m := Message[T]{
				Payload:     savedMessages[i].Payload.(T),
				ScheduledAt: savedMessages[i].ScheduledAt,
			}
			if savedMessages[i].ScheduleTime.Before(now) {
				s.msgs <- m
			} else {
				s.schedule(savedMessages[i].ID, savedMessages[i].ScheduleTime, m)
			}
		}

		select {
		case <-s.ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
		}
	}
}

func (s Sked[T]) schedule(id string, schedTime time.Time, msg Message[T]) {
	go func() {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(schedTime.Sub(time.Now())):
			s.msgs <- msg
			err := s.store.Delete(s.ctx, id)
			if err != nil {
				s.sendErr(err)
			}
		}
	}()
}

func (s Sked[T]) sendErr(e error) {
	if s.errChan == nil {
		return
	}

	select {
	case s.errChan <- e:
	default:
	}
}
