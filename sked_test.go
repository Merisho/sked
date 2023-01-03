package sked

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

func TestSked(t *testing.T) {
	suite.Run(t, new(SkedTestSuite))
}

type SkedTestSuite struct {
	suite.Suite
}

func (ts *SkedTestSuite) TestScheduleForPastDate() {
	sked := NewSked[any](context.Background(), NewTestStore(), Options{})

	_, err := sked.Schedule(time.Now().Add(-1*time.Hour), nil)
	ts.EqualError(err, "cannot schedule for past date")
}

func (ts *SkedTestSuite) TestSchedule() {
	sked := NewSked[string](context.Background(), NewTestStore(), Options{})

	scheduledAt := time.Now()
	_, err := sked.Schedule(time.Now().Add(200*time.Millisecond), "this is the payload")
	ts.NoError(err)

	msg := <-sked.Messages()
	ts.Equal(msg.Payload, "this is the payload")
	ts.GreaterOrEqual(time.Now().UnixNano()-scheduledAt.UnixNano(), int64(200*time.Millisecond))
}

func (ts *SkedTestSuite) TestDoNotSendTheSameMoreThanOnce() {
	store := NewTestStore()
	sked := NewSked[string](context.Background(), store, Options{})

	_, err := sked.Schedule(time.Now().Add(200*time.Millisecond), "this is the payload")
	ts.NoError(err)

	select {
	case <-sked.Messages():
	case <-time.After(time.Second):
		ts.Fail("timeout waiting for message")
	}

	select {
	case <-sked.Messages():
		ts.Fail("should not have received the message more than once")
	case <-time.After(500 * time.Millisecond):
	}
}

func (ts *SkedTestSuite) TestCancellation() {
	store := NewTestStore()
	sked := NewSked[string](context.Background(), store, Options{
		RefreshInterval: 500 * time.Millisecond,
	})
	// schedule time is greater than RefreshInterval,
	// so the message won't have been captured by the scheduler at all when we Cancel later
	_, err := sked.Schedule(time.Now().Add(time.Second), "this is the payload 1")
	ts.NoError(err)

	// this will be taken from the store and scheduled
	// because schedule time is less than RefreshInterval
	// then it must be cancelled
	_, err = sked.Schedule(time.Now().Add(400*time.Millisecond), "this is the payload 2")
	ts.NoError(err)

	sked.Cancel()

	select {
	case <-sked.Messages():
		ts.Fail("should not have received any messages after Cancel")
	case <-time.After(time.Second):
	}
}

func (ts *SkedTestSuite) TestDurability() {
	store := NewTestStore()
	sked := NewSked[string](context.Background(), store, Options{
		RefreshInterval: 500 * time.Millisecond,
	})

	scheduledAt := time.Now().Add(3 * time.Second)
	_, err := sked.Schedule(scheduledAt, "this is the payload")
	ts.NoError(err)

	sked.Cancel()

	sked = NewSked[string](context.Background(), store, Options{
		RefreshInterval: 4 * time.Second,
	})

	select {
	case <-sked.Messages():
	case <-time.After(4 * time.Second):
		ts.Fail("should have received the message with the new instance of the scheduler")
	}
}

func (ts *SkedTestSuite) TestReturnIfCancellationReceivedWhenConstantlyGettingStorageErrors() {
	store := NewTestStore()
	store.errorOnCapture()

	errs := make(chan error, 100)
	sked := NewSked[string](context.Background(), store, Options{
		RefreshInterval: 500 * time.Millisecond,
		Errors:          errs,
	})

	ts.Eventually(func() bool {
		<-errs
		return true
	}, 1*time.Second, 100*time.Millisecond)

	sked.Cancel()

	var err error
	ts.Eventually(func() bool {
		err = <-errs
		return err == context.Canceled
	}, 1*time.Second, 100*time.Millisecond)
}

func NewTestStore() *TestStore {
	return &TestStore{}
}

type TestStore struct {
	mu         sync.Mutex
	msgs       []SavedMessage
	captureErr bool
}

func (s *TestStore) Put(_ context.Context, msg SavedMessage) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg.ID = strconv.Itoa(time.Now().Nanosecond())
	s.msgs = append(s.msgs, msg)
	return msg.ID, nil
}

func (s *TestStore) errorOnCapture() {
	s.captureErr = true
}

func (s *TestStore) Capture(_ context.Context, messages []SavedMessage, t time.Time) (int, string, error) {
	if s.captureErr {
		return 0, "", errors.New("storage error")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	msgNum := 0
	for i, msg := range s.msgs {
		if msg.ScheduleTime.Before(t) {
			s.msgs = append(s.msgs[:i], s.msgs[i+1:]...)
			messages[msgNum] = msg
			msgNum++
		}

		if msgNum >= len(messages) {
			break
		}
	}

	return msgNum, "", nil
}

func (s *TestStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.msgs {
		if s.msgs[i].ID == id {
			s.msgs = append(s.msgs[:i], s.msgs[i+1:]...)
			return nil
		}
	}
	return nil
}

func (s *TestStore) DeleteLocked(ctx context.Context, lockID string) error {
	return nil
}
