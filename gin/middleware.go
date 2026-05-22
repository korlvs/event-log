package gin

import (
	"github.com/gin-gonic/gin"
	"github.com/korlvs/event-logging/libs/go-outbox"
)

// RequestMetadata возвращает middleware для Gin.
func RequestMetadata() gin.HandlerFunc {
	return func(c *gin.Context) {
		meta := outbox.ExtractRequestMetadata(c.Request)
		ctx := outbox.ContextWithRequestMetadata(c.Request.Context(), meta)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
