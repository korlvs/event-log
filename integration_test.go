package outbox

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	eventpb "github.com/korlvs/event-contract"
	_ "github.com/lib/pq"
)

// Интеграционные тесты требуют реального PostgreSQL.
// DSN передаётся через OUTBOX_TEST_DSN, иначе тест пропускается.

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DSN not set, skipping integration test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS outbox")
		db.Exec("DROP TABLE IF EXISTS outbox_schema_migrations")
		db.Close()
	})
	// Чистый старт.
	db.Exec("DROP TABLE IF EXISTS outbox")
	db.Exec("DROP TABLE IF EXISTS outbox_schema_migrations")
	return db
}

func newEvent() *eventpb.Event {
	return NewEvent("alm", "system.created", eventpb.OperationType_OPERATION_TYPE_CREATE, eventpb.EventStatus_EVENT_STATUS_SUCCESS)
}

func countRows(t *testing.T, db *sql.DB, where string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM outbox WHERE " + where).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestIntegrationInitCreatesTable(t *testing.T) {
	db := openTestDB(t)
	o, err := initOutbox(db, Config{Schema: "public", Mode: "binary"})
	if err != nil {
		t.Fatalf("initOutbox: %v", err)
	}
	resetForTest(o)

	// Таблица должна существовать сразу после Init.
	var exists bool
	if err := db.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='outbox')").Scan(&exists); err != nil {
		t.Fatalf("check: %v", err)
	}
	if !exists {
		t.Fatal("outbox table must exist after init (eager ensureTable)")
	}
	// Первое событие не теряется.
	if err := PublishEvent(context.Background(), "k", newEvent()); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if countRows(t, db, "TRUE") != 1 {
		t.Fatal("first event lost")
	}
}

func TestIntegrationTransactionalOutbox(t *testing.T) {
	db := openTestDB(t)
	o, err := initOutbox(db, Config{Schema: "public", Mode: "binary"})
	if err != nil {
		t.Fatalf("initOutbox: %v", err)
	}
	resetForTest(o)

	// Rollback откатывает запись.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	exec := ExecutorFunc(func(c context.Context, q string, a ...any) error {
		_, e := tx.ExecContext(c, q, a...)
		return e
	})
	if err := PublishEventWithExecutor(context.Background(), exec, "k1", newEvent()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	tx.Rollback()
	if countRows(t, db, "TRUE") != 0 {
		t.Fatal("rollback did not discard outbox row")
	}

	// Commit сохраняет запись.
	tx2, _ := db.Begin()
	exec2 := ExecutorFunc(func(c context.Context, q string, a ...any) error {
		_, e := tx2.ExecContext(c, q, a...)
		return e
	})
	if err := PublishEventWithExecutor(context.Background(), exec2, "k2", newEvent()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if countRows(t, db, "TRUE") != 1 {
		t.Fatal("commit did not persist outbox row")
	}
}

func TestIntegrationCleanup(t *testing.T) {
	db := openTestDB(t)
	o, err := initOutbox(db, Config{Schema: "public", Mode: "binary"})
	if err != nil {
		t.Fatalf("initOutbox: %v", err)
	}
	resetForTest(o)

	// published старше retention.
	db.Exec("INSERT INTO outbox (event_key, payload, published_at) VALUES ('old', '\\x01', NOW() - INTERVAL '2 hours')")
	// published свежий.
	db.Exec("INSERT INTO outbox (event_key, payload, published_at) VALUES ('fresh', '\\x01', NOW())")
	// неотправленный.
	db.Exec("INSERT INTO outbox (event_key, payload) VALUES ('pending', '\\x01')")
	// failed.
	db.Exec("INSERT INTO outbox (event_key, payload, failed_at) VALUES ('failed', '\\x01', NOW())")

	w := NewWorker(db, o, Config{Schema: "public", CleanupRetention: time.Hour})
	w.cleanup()

	if n := countRows(t, db, "event_key='old'"); n != 0 {
		t.Fatal("old published row should be removed")
	}
	if n := countRows(t, db, "event_key='fresh'"); n != 1 {
		t.Fatal("fresh published row should remain")
	}
	if n := countRows(t, db, "event_key='pending'"); n != 1 {
		t.Fatal("pending row should remain")
	}
	if n := countRows(t, db, "event_key='failed'"); n != 1 {
		t.Fatal("failed row should remain")
	}
}

func TestIntegrationBacklogWarn(t *testing.T) {
	db := openTestDB(t)
	o, err := initOutbox(db, Config{Schema: "public", Mode: "binary"})
	if err != nil {
		t.Fatalf("initOutbox: %v", err)
	}
	resetForTest(o)

	for i := 0; i < 3; i++ {
		db.Exec("INSERT INTO outbox (event_key, payload) VALUES ('p', '\\x01')")
	}
	w := NewWorker(db, o, Config{Schema: "public", BacklogWarnThreshold: 2, BacklogCheckInterval: time.Nanosecond})

	logs := captureLog(func() { w.checkBacklog(context.Background(), `"public"."outbox"`) })
	if !strings.Contains(logs, "WARN") || !strings.Contains(logs, "backlog") {
		t.Fatalf("expected backlog WARN, got %q", logs)
	}
}
