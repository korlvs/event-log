package outbox

import (
	"bytes"
	"database/sql"
	"log"
	"os"
	"sync"
)

// resetForTest подменяет глобальный синглтон и сбрасывает sync.Once,
// чтобы тесты могли переинициализировать библиотеку. White-box only.
func resetForTest(o *Outbox) {
	globalInstance = o
	once = sync.Once{}
}

// newTestOutbox конструирует Outbox без вызова setup (без БД и сети).
// Применяет дефолты конфига, чтобы buildInsert/encode-логика были корректны.
func newTestOutbox(db *sql.DB, cfg Config) *Outbox {
	_ = cfg.validateAndDefault()
	return &Outbox{db: db, cfg: cfg, senderBroken: true}
}

// captureLog перехватывает вывод stdlib log на время выполнения fn и
// возвращает накопленный текст.
func captureLog(fn func()) string {
	var buf bytes.Buffer
	flags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(flags)
	}()
	fn()
	return buf.String()
}
