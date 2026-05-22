package outbox

import (
	"context"

	"github.com/google/uuid"
	eventpb "github.com/korlvs/event-contract"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NewEvent создаёт базовое событие с обязательными полями.
func NewEvent(category, action string, opType eventpb.OperationType, status eventpb.EventStatus) *eventpb.Event {
	now := timestamppb.Now()
	return &eventpb.Event{
		EventId:       uuid.New().String(),
		Timestamp:     now,
		Category:      category,
		Action:        action,
		OperationType: opType,
		Status:        status,
		SchemaVersion: "1.0",
	}
}

// SetActor добавляет информацию об инициаторе.
func SetActor(event *eventpb.Event, id, typ, displayName string) {
	if event.Actor == nil {
		event.Actor = &eventpb.Actor{}
	}
	event.Actor.Id = id
	event.Actor.Type = typ
	event.Actor.DisplayName = displayName
}

// SetResource добавляет целевой ресурс.
func SetResource(event *eventpb.Event, id, typ string) {
	if event.Resource == nil {
		event.Resource = &eventpb.Target{}
	}
	event.Resource.Id = id
	event.Resource.Type = typ
}

// SetResourceDetails устанавливает бизнес-данные в виде map.
func SetResourceDetails(event *eventpb.Event, details map[string]interface{}) error {
	if details == nil {
		return nil
	}
	structDetails, err := structpb.NewStruct(details)
	if err != nil {
		return err
	}
	event.ResourceDetails = structDetails
	return nil
}

// SetDetails устанавливает технические детали.
func SetDetails(event *eventpb.Event, details map[string]interface{}) error {
	if details == nil {
		return nil
	}
	structDetails, err := structpb.NewStruct(details)
	if err != nil {
		return err
	}
	event.Details = structDetails
	return nil
}

// PublishSimple упрощённая публикация события без actor и resource.
func PublishSimple(ctx context.Context, category, action string, opType eventpb.OperationType, status eventpb.EventStatus) error {
	event := NewEvent(category, action, opType, status)
	return PublishEvent(ctx, event.EventId, event)
}
