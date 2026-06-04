package outbox

import (
	"context"
	"testing"

	eventpb "github.com/korlvs/event-contract"
)

func TestEnrichEventDefaults(t *testing.T) {
	o := newTestOutbox(nil, Config{ServiceName: "alm", Environment: "development", ServiceVersion: "1.2.3"})
	resetForTest(o)

	ev := &eventpb.Event{}
	enrichEvent(context.Background(), ev)

	if ev.SchemaVersion != "1.0" {
		t.Fatalf("SchemaVersion = %q, want 1.0", ev.SchemaVersion)
	}
	if ev.Timestamp == nil {
		t.Fatal("Timestamp should be set")
	}
	if ev.Context == nil || ev.Context.SourceService != "alm" {
		t.Fatalf("SourceService not set from config")
	}
	if ev.Context.Environment != "development" {
		t.Fatalf("Environment = %q, want development", ev.Context.Environment)
	}
	if ev.Actor == nil || ev.Actor.Type != "anonymous" {
		t.Fatalf("Actor.Type = %v, want anonymous (no metadata, no id)", ev.Actor)
	}
	if ev.Details == nil {
		t.Fatal("Details should be initialised")
	}
	if v := ev.Details.Fields["service_version"]; v == nil || v.GetStringValue() != "1.2.3" {
		t.Fatalf("service_version detail missing or wrong: %v", v)
	}
	if v := ev.Details.Fields["environment"]; v == nil || v.GetStringValue() != "development" {
		t.Fatalf("environment detail missing or wrong: %v", v)
	}
}

func TestEnrichEventFromMetadata(t *testing.T) {
	o := newTestOutbox(nil, Config{ServiceName: "alm"})
	resetForTest(o)

	ctx := ContextWithRequestMetadata(context.Background(), &RequestMetadata{
		ClientIP:      "10.0.0.1",
		CorrelationID: "corr-1",
		UserAgent:     "curl",
		UserID:        "user-42",
		UserEmail:     "u@example.com",
	})
	ev := &eventpb.Event{}
	enrichEvent(ctx, ev)

	if ev.Context.ClientIp != "10.0.0.1" {
		t.Fatalf("ClientIp = %q", ev.Context.ClientIp)
	}
	if ev.Context.CorrelationId != "corr-1" {
		t.Fatalf("CorrelationId = %q", ev.Context.CorrelationId)
	}
	if ev.Actor.Id != "user-42" || ev.Actor.DisplayName != "u@example.com" {
		t.Fatalf("actor from metadata wrong: %+v", ev.Actor)
	}
	if ev.Actor.Type != "user" {
		t.Fatalf("Actor.Type = %q, want user (id present)", ev.Actor.Type)
	}
}

func TestEnrichEventDoesNotOverrideExplicitActor(t *testing.T) {
	o := newTestOutbox(nil, Config{ServiceName: "alm"})
	resetForTest(o)

	ev := &eventpb.Event{Actor: &eventpb.Actor{Type: "system", DisplayName: "system@alm"}}
	enrichEvent(context.Background(), ev)

	if ev.Actor.Type != "system" || ev.Actor.DisplayName != "system@alm" {
		t.Fatalf("explicit system actor overwritten: %+v", ev.Actor)
	}
}
