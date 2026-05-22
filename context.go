package outbox

import "context"

type contextKey string

const metadataKey contextKey = "outbox_request_metadata"

type RequestMetadata struct {
	ClientIP       string
	UserAgent      string
	Referer        string
	AcceptLanguage string
	CorrelationID  string
	UserID         string
	UserEmail      string
}

func ContextWithRequestMetadata(ctx context.Context, meta *RequestMetadata) context.Context {
	return context.WithValue(ctx, metadataKey, meta)
}

func RequestMetadataFromContext(ctx context.Context) *RequestMetadata {
	if val := ctx.Value(metadataKey); val != nil {
		if meta, ok := val.(*RequestMetadata); ok {
			return meta
		}
	}
	return nil
}
