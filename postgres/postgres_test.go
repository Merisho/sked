package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sked"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/suite"
)

func now() time.Time {
	return time.Now().UTC()
}

func TestPostgres(t *testing.T) {
	suite.Run(t, new(PostgresTestSuite))
}

type PostgresTestSuite struct {
	suite.Suite
	db *sql.DB
}

func (ts *PostgresTestSuite) SetupSuite() {
	db, err := sql.Open("pgx", "postgres://sked:sked@localhost:5432/sked?sslmode=disable")
	if err != nil {
		panic(err)
	}

	ts.db = db
}

func (ts *PostgresTestSuite) TestBootstrapSchema() {
	tableName := ts.testTableName()
	pg := NewStore(ts.db, tableName, Config{})
	err := pg.Bootstrap(context.Background())
	ts.Require().NoError(err)

	var cnt int
	q := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '%s'", tableName)
	err = ts.db.QueryRow(q).Scan(&cnt)
	ts.Require().NoError(err)
	ts.Require().Equal(1, cnt)
}

func (ts *PostgresTestSuite) TestPut() {
	pg := NewStore(ts.db, ts.testTableName(), Config{})
	err := pg.Bootstrap(context.Background())
	ts.Require().NoError(err)

	id, err := pg.Put(context.Background(), sked.SavedMessage{
		Payload:      "test",
		ScheduleTime: now().Add(time.Hour),
		ScheduledAt:  now(),
	})
	ts.Require().NoError(err)

	_, err = uuid.Parse(id)
	ts.Require().NoError(err)
}

func (ts *PostgresTestSuite) TestDelete() {
	tableName := ts.testTableName()
	pg := NewStore(ts.db, tableName, Config{})
	err := pg.Bootstrap(context.Background())
	ts.Require().NoError(err)

	id, err := pg.Put(context.Background(), sked.SavedMessage{
		Payload:      "test",
		ScheduleTime: now().Add(time.Hour),
		ScheduledAt:  now(),
	})
	ts.Require().NoError(err)

	err = pg.Delete(context.Background(), id)
	ts.Require().NoError(err)

	var cnt int
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id=$1", tableName)
	err = ts.db.QueryRow(q, id).Scan(&cnt)
	ts.Require().NoError(err)
	ts.Require().Equal(0, cnt)
}

func (ts *PostgresTestSuite) TestCapture_MustNotCaptureAlreadyLocked() {
	ctx := context.Background()
	testTable := ts.testTableName()
	pg := NewStore(ts.db, testTable, Config{})
	err := pg.Bootstrap(ctx)
	ts.Require().NoError(err)

	_, err = pg.Put(ctx, sked.SavedMessage{
		Payload:      "test 1",
		ScheduleTime: now().Add(10 * time.Second),
		ScheduledAt:  now(),
	})
	ts.Require().NoError(err)

	_, err = pg.Put(ctx, sked.SavedMessage{
		Payload:      "test 2",
		ScheduleTime: now().Add(10 * time.Second),
		ScheduledAt:  now(),
	})
	ts.Require().NoError(err)

	messages := make([]sked.SavedMessage, 1)
	n, _, err := pg.Capture(ctx, messages, now().Add(10*time.Second))
	ts.Require().NoError(err)
	ts.Require().Equal(1, n)

	msg1 := messages[0]

	n, _, err = pg.Capture(ctx, messages, now().Add(10*time.Second))
	ts.Require().NoError(err)
	ts.Require().Equal(1, n)
	ts.Require().NotEqual(msg1.ID, messages[0].ID)
	ts.Require().NotEqual(msg1.LockID, messages[0].LockID)
}

func (ts *PostgresTestSuite) TestCapture_MustRecaptureWithExpiredLock() {
	ctx := context.Background()
	tableName := ts.testTableName()
	pg := NewStore(ts.db, tableName, Config{
		LockDuration: time.Minute,
	})
	err := pg.Bootstrap(ctx)
	ts.Require().NoError(err)

	_, err = pg.Put(ctx, sked.SavedMessage{
		Payload:      "test 1",
		ScheduleTime: now().Add(10 * time.Second),
		ScheduledAt:  now(),
	})
	ts.Require().NoError(err)

	messages := make([]sked.SavedMessage, 1)
	n, lockID1, err := pg.Capture(ctx, messages, now().Add(10*time.Second))
	ts.Require().NoError(err)
	ts.Require().Equal(1, n)

	msg1 := messages[0]

	q := fmt.Sprintf("UPDATE %s SET locked_at=$1 WHERE id=$2", tableName)
	_, err = ts.db.ExecContext(ctx, q, now().Add(-time.Hour), msg1.ID)
	ts.Require().NoError(err)

	n, lockID2, err := pg.Capture(ctx, messages, now().Add(10*time.Second))
	ts.Require().NoError(err)
	ts.Require().Equal(1, n)
	ts.Require().Equal(msg1.ID, messages[0].ID)
	ts.Require().NotEqual(lockID1, lockID2)
}

func (ts *PostgresTestSuite) TestCapture_MustReturnErrorIfCapturingFutureMessagesThatWillHaveLockExpired() {
	ctx := context.Background()
	tableName := ts.testTableName()
	pg := NewStore(ts.db, tableName, Config{
		LockDuration: time.Minute,
	})
	err := pg.Bootstrap(ctx)
	ts.Require().NoError(err)

	_, err = pg.Put(ctx, sked.SavedMessage{
		Payload:      "test 1",
		ScheduleTime: now().Add(time.Hour),
		ScheduledAt:  now(),
	})
	ts.Require().NoError(err)

	messages := make([]sked.SavedMessage, 1)
	n, _, err := pg.Capture(ctx, messages, now().Add(time.Hour))
	ts.Require().Equal(ErrCaptureTimeThresholdGreaterThanLockDuration, err)
	ts.Require().Zero(n)
}

func (ts *PostgresTestSuite) testTableName() string {
	return fmt.Sprintf("test_%d", now().UnixNano())
}
