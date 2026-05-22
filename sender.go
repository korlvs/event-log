package outbox

import "context"

type Sender interface {
	Send(ctx context.Context, key string, encodedKey, encodedValue []byte) error
}
