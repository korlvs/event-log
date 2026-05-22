package std

import (
	"net/http"

	"github.com/korlvs/event-logging/libs/go-outbox"
)

// Middleware оборачивает http.Handler и добавляет метаданные запроса в контекст.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		meta := outbox.ExtractRequestMetadata(r)
		ctx := outbox.ContextWithRequestMetadata(r.Context(), meta)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
