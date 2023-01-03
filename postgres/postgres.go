package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"sked"
)

var (
	ErrCaptureTimeThresholdGreaterThanLockDuration = errors.New("capture time threshold must be less than lock duration as compared to current time")
)

var _ sked.Store = (*Store)(nil)
var _ DB = (*sql.DB)(nil)

type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

func NewStore(db DB, name string, conf Config) *Store {
	conf = conf.prepare()
	return &Store{
		tableName: name,
		db:        db,
		conf:      conf,
	}
}

type Store struct {
	tableName string
	db        DB
	conf      Config
}

func (s *Store) Bootstrap(ctx context.Context) error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	payload JSONB NOT NULL,
	schedule_time TIMESTAMP NOT NULL,
	scheduled_at TIMESTAMP NOT NULL,
	lock_id UUID,
	locked_at TIMESTAMP
)`, s.tableName)
	_, err := s.db.ExecContext(ctx, q)
	if err != nil {
		return err
	}

	q = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_schedule_time_idx ON %s (schedule_time)`, s.tableName, s.tableName)
	_, err = s.db.ExecContext(ctx, q)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) Put(ctx context.Context, msg sked.SavedMessage) (string, error) {
	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return "", err
	}

	var id string
	q := fmt.Sprintf(`INSERT INTO %s
			(id, payload, schedule_time, scheduled_at) VALUES (gen_random_uuid(), $1, $2, $3)
			RETURNING id`, s.tableName)
	err = s.db.QueryRowContext(ctx, q, payload, msg.ScheduleTime, msg.ScheduledAt).Scan(&id)
	if err != nil {
		return "", err
	}

	return id, nil
}

func (s *Store) Capture(ctx context.Context, messages []sked.SavedMessage, beforeThisTime time.Time) (n int, lockID string, err error) {
	if beforeThisTime.Sub(time.Now().UTC()) > s.conf.LockDuration {
		return 0, "", ErrCaptureTimeThresholdGreaterThanLockDuration
	}

	limit := len(messages)

	q := fmt.Sprintf(`UPDATE %s SET lock_id=gen_random_uuid(), locked_at=NOW()
								WHERE id IN (
									SELECT id FROM %s WHERE schedule_time<=$1 AND (lock_id IS NULL OR locked_at <= $2)
									FOR UPDATE SKIP LOCKED LIMIT $3
								) RETURNING id, payload, schedule_time, scheduled_at, lock_id`, s.tableName, s.tableName)
	lockExpirationTime := time.Now().UTC().Add(-s.conf.LockDuration)
	res, err := s.db.QueryContext(ctx, q, beforeThisTime, lockExpirationTime, limit)
	if err != nil {
		return 0, "", err
	}
	defer res.Close()

	for res.Next() && n < limit {
		var msg sked.SavedMessage
		var payload []byte
		err := res.Scan(&msg.ID, &payload, &msg.ScheduleTime, &msg.ScheduledAt, &msg.LockID)
		if err != nil {
			return 0, "", err
		}

		err = json.Unmarshal(payload, &msg.Payload)
		if err != nil {
			return 0, "", err
		}

		messages[n] = msg
		n++
	}

	if n > 0 {
		lockID = messages[0].LockID
	}

	return n, lockID, nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	q := fmt.Sprintf(`DELETE FROM %s WHERE id=$1`, s.tableName)
	_, err := s.db.ExecContext(ctx, q, id)
	return err
}

func (s *Store) DeleteLocked(ctx context.Context, lockID string) error {
	q := fmt.Sprintf(`DELETE FROM %s WHERE lock_id=$1`, s.tableName)
	_, err := s.db.ExecContext(ctx, q, lockID)
	return err
}
