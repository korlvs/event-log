package echo

import (
	"github.com/korlvs/event-logging/libs/go-outbox"
	"github.com/labstack/echo/v4"
)

func RequestMetadata() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			meta := outbox.ExtractRequestMetadata(c.Request())
			ctx := outbox.ContextWithRequestMetadata(c.Request().Context(), meta)
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
}
