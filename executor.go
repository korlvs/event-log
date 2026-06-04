package outbox

import "context"

// Executor — нейтральный интерфейс записи одной операции INSERT.
// Совместим с database/sql, pgx и моком за счёт того, что результат не
// возвращается (INSERT в outbox его не использует). Это позволяет хосту
// участвовать в собственной транзакции, не притягивая зависимости в библиотеку.
type Executor interface {
	Exec(ctx context.Context, sql string, args ...any) error
}

// ExecutorFunc — адаптер обычной функции к интерфейсу Executor.
type ExecutorFunc func(ctx context.Context, sql string, args ...any) error

func (f ExecutorFunc) Exec(ctx context.Context, sql string, args ...any) error {
	return f(ctx, sql, args...)
}
