package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"

	eventpb "github.com/korlvs/event-contract"
)

func TestExecutorFuncDelegates(t *testing.T) {
	called := false
	var gotSQL string
	var gotArgs []any
	f := ExecutorFunc(func(_ context.Context, sql string, args ...any) error {
		called = true
		gotSQL = sql
		gotArgs = args
		return nil
	})
	if err := f.Exec(context.Background(), "SELECT 1", 42); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("ExecutorFunc did not delegate")
	}
	if gotSQL != "SELECT 1" || len(gotArgs) != 1 || gotArgs[0] != 42 {
		t.Fatalf("delegated args mismatch: sql=%q args=%v", gotSQL, gotArgs)
	}
}

func TestPublishEventWithExecutorNotInitialized(t *testing.T) {
	resetForTest(nil)
	err := PublishEventWithExecutor(context.Background(), ExecutorFunc(func(context.Context, string, ...any) error {
		return nil
	}), "k", &eventpb.Event{})
	if !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("err = %v, want ErrNotInitialized", err)
	}
}

func TestPublishEventWithExecutorBuildsSQL(t *testing.T) {
	tests := []struct {
		name      string
		storeJSON bool
		schema    string
		wantSQL   string
		wantArgs  int
	}{
		{
			name:     "no json public schema",
			wantSQL:  `INSERT INTO "public"."outbox" (event_key, payload) VALUES ($1, $2)`,
			wantArgs: 2,
		},
		{
			name:      "with json",
			storeJSON: true,
			wantSQL:   `INSERT INTO "public"."outbox" (event_key, payload, payload_json) VALUES ($1, $2, $3)`,
			wantArgs:  3,
		},
		{
			name:     "custom schema",
			schema:   "audit",
			wantSQL:  `INSERT INTO "audit"."outbox" (event_key, payload) VALUES ($1, $2)`,
			wantArgs: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newTestOutbox(nil, Config{StoreJSON: tt.storeJSON, Schema: tt.schema, ServiceName: "alm"})
			resetForTest(o)

			var gotSQL string
			var gotArgs []any
			exec := ExecutorFunc(func(_ context.Context, sql string, args ...any) error {
				gotSQL = sql
				gotArgs = args
				return nil
			})
			ev := NewEvent("alm", "system.created", eventpb.OperationType_OPERATION_TYPE_CREATE, eventpb.EventStatus_EVENT_STATUS_SUCCESS)
			if err := PublishEventWithExecutor(context.Background(), exec, "key-1", ev); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotSQL != tt.wantSQL {
				t.Fatalf("sql = %q, want %q", gotSQL, tt.wantSQL)
			}
			if len(gotArgs) != tt.wantArgs {
				t.Fatalf("args len = %d, want %d", len(gotArgs), tt.wantArgs)
			}
			if gotArgs[0] != "key-1" {
				t.Fatalf("first arg = %v, want key-1", gotArgs[0])
			}
			if o.hasDBProblem() {
				t.Fatal("dbProblem should be false after success")
			}
		})
	}
}

func TestPublishEventWithExecutorErrorSetsDBProblem(t *testing.T) {
	o := newTestOutbox(nil, Config{Schema: "public"})
	resetForTest(o)

	wantErr := errors.New("db down")
	exec := ExecutorFunc(func(context.Context, string, ...any) error {
		return wantErr
	})
	ev := NewEvent("alm", "system.created", eventpb.OperationType_OPERATION_TYPE_CREATE, eventpb.EventStatus_EVENT_STATUS_SUCCESS)
	err := PublishEventWithExecutor(context.Background(), exec, "k", ev)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if !o.hasDBProblem() {
		t.Fatal("dbProblem should be true after executor failure")
	}
}

func TestPublishEventWithExecutorClearsDBProblem(t *testing.T) {
	o := newTestOutbox(nil, Config{Schema: "public"})
	o.markDBProblem()
	resetForTest(o)

	logs := captureLog(func() {
		exec := ExecutorFunc(func(context.Context, string, ...any) error { return nil })
		ev := NewEvent("alm", "a", eventpb.OperationType_OPERATION_TYPE_CREATE, eventpb.EventStatus_EVENT_STATUS_SUCCESS)
		if err := PublishEventWithExecutor(context.Background(), exec, "k", ev); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if o.hasDBProblem() {
		t.Fatal("dbProblem should be cleared after success")
	}
	if !strings.Contains(logs, "DB recovered") {
		t.Fatalf("expected recovery log, got %q", logs)
	}
}
