package outbox

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

type errEncoder struct{ err error }

func (e errEncoder) Encode(string, []byte) ([]byte, []byte, error) { return nil, nil, e.err }

type okEncoder struct{}

func (okEncoder) Encode(key string, b []byte) ([]byte, []byte, error) { return []byte(key), b, nil }

type stubSender struct{ err error }

func (s stubSender) Send(context.Context, string, []byte, []byte) error { return s.err }

func newWorkerWithMockDB(t *testing.T, cfg Config) (*Worker, *Outbox, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	o := newTestOutbox(db, cfg)
	w := NewWorker(db, o, o.cfg)
	return w, o, mock, func() { db.Close() }
}

func TestWorkerEncodeFailureBelowThreshold(t *testing.T) {
	w, o, mock, closeFn := newWorkerWithMockDB(t, Config{Schema: "public", MaxEncodeAttempts: 3})
	defer closeFn()
	o.encoder = errEncoder{err: errors.New("bad proto")}
	o.sender = stubSender{}
	o.senderBroken = false

	mock.ExpectQuery("SELECT id, event_key, payload, attempts FROM").
		WithArgs(o.cfg.BatchSize).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_key", "payload", "attempts"}).
			AddRow("id-1", "key-1", []byte{1, 2}, 0))
	// attempts 0 -> 1, below threshold 3: only attempts/last_error update
	mock.ExpectExec(regexp.QuoteMeta("UPDATE")).
		WithArgs(1, "bad proto", "id-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	w.processBatch()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestWorkerEncodeFailureAtThreshold(t *testing.T) {
	w, o, mock, closeFn := newWorkerWithMockDB(t, Config{Schema: "public", MaxEncodeAttempts: 3})
	defer closeFn()
	o.encoder = errEncoder{err: errors.New("bad proto")}
	o.sender = stubSender{}
	o.senderBroken = false

	mock.ExpectQuery("SELECT id, event_key, payload, attempts FROM").
		WithArgs(o.cfg.BatchSize).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_key", "payload", "attempts"}).
			AddRow("id-1", "key-1", []byte{1, 2}, 2))
	// attempts 2 -> 3 == threshold: failed_at must be set
	mock.ExpectExec("UPDATE .* SET attempts=.*failed_at=NOW.*last_error").
		WithArgs(3, "bad proto", "id-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	w.processBatch()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestWorkerSendErrorBreaksBatch(t *testing.T) {
	w, o, mock, closeFn := newWorkerWithMockDB(t, Config{Schema: "public", MaxEncodeAttempts: 3})
	defer closeFn()
	o.encoder = okEncoder{}
	o.sender = stubSender{err: errors.New("kafka down")}
	o.senderBroken = false

	mock.ExpectQuery("SELECT id, event_key, payload, attempts FROM").
		WithArgs(o.cfg.BatchSize).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_key", "payload", "attempts"}).
			AddRow("id-1", "key-1", []byte{1, 2}, 0).
			AddRow("id-2", "key-2", []byte{3, 4}, 0))
	// Send fails on first record -> MarkSenderBroken + return, no UPDATE published, no attempts bump

	w.processBatch()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
	if w.outbox.GetSender() != nil {
		t.Fatal("sender should be marked broken after send failure")
	}
}

func TestWorkerSelectExcludesFailed(t *testing.T) {
	w, o, mock, closeFn := newWorkerWithMockDB(t, Config{Schema: "public"})
	defer closeFn()
	o.encoder = okEncoder{}
	o.sender = stubSender{}
	o.senderBroken = false

	mock.ExpectQuery("WHERE published_at IS NULL AND failed_at IS NULL").
		WithArgs(o.cfg.BatchSize).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_key", "payload", "attempts"}))

	w.processBatch()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestWorkerCleanup(t *testing.T) {
	w, _, mock, closeFn := newWorkerWithMockDB(t, Config{Schema: "public", CleanupEnabled: true, CleanupRetention: time.Hour})
	defer closeFn()

	mock.ExpectExec("DELETE FROM .* WHERE published_at IS NOT NULL AND published_at <").
		WithArgs("3600 seconds").
		WillReturnResult(sqlmock.NewResult(0, 5))

	w.cleanup()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestWorkerBacklogWarn(t *testing.T) {
	w, _, mock, closeFn := newWorkerWithMockDB(t, Config{Schema: "public", BacklogWarnThreshold: 2})
	defer closeFn()

	mock.ExpectQuery("SELECT COUNT.*MIN.*WHERE published_at IS NULL AND failed_at IS NULL").
		WillReturnRows(sqlmock.NewRows([]string{"count", "min"}).AddRow(5, nil))

	logs := captureLog(func() { w.checkBacklog(context.Background(), `"public"."outbox"`) })

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
	if !regexp.MustCompile(`WARN .* backlog 5 unpublished`).MatchString(logs) {
		t.Fatalf("expected WARN backlog log, got %q", logs)
	}
}
