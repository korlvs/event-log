package gin

import (
	"github.com/gin-gonic/gin"
	outbox "github.com/korlvs/event-log"
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
