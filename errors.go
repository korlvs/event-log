package outbox

import "errors"

var (
	ErrNotInitialized = errors.New("outbox: not initialized")
)
