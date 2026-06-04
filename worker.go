package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

type Worker struct {
	db              *sql.DB
	outbox          *Outbox
	cfg             Config
	stopCh          chan struct{}
	lastBacklogTime time.Time
}

func NewWorker(db *sql.DB, outbox *Outbox, cfg Config) *Worker {
	return &Worker{
		db:     db,
		outbox: outbox,
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

func (w *Worker) Start() {
	if w.cfg.EnableConsoleLogging {
		log.Printf("outbox worker: started, interval=%s", w.cfg.deliverInterval())
	}
	deliver := time.NewTicker(w.cfg.deliverInterval())
	defer deliver.Stop()

	var cleanupC <-chan time.Time
	if w.cfg.CleanupEnabled {
		cleanup := time.NewTicker(w.cfg.CleanupInterval)
		defer cleanup.Stop()
		cleanupC = cleanup.C
	}

	for {
		select {
		case <-deliver.C:
			w.processBatch()
		case <-cleanupC:
			w.cleanup()
		case <-w.stopCh:
			if w.cfg.EnableConsoleLogging {
				log.Println("outbox worker: stopping")
			}
			return
		}
	}
}

func (w *Worker) processBatch() {
	ctx := context.Background()

	if w.outbox.hasDBProblem() {
		if err := w.outbox.ensureTable(); err != nil {
			log.Printf("outbox worker: DB still unavailable: %v", err)
			return
		}
	}

	tableOutbox := fullTableName(w.cfg.Schema, "outbox")

	w.checkBacklog(ctx, tableOutbox)

	selectQuery := fmt.Sprintf(
		`SELECT id, event_key, payload, attempts FROM %s
         WHERE published_at IS NULL AND failed_at IS NULL
         ORDER BY created_at ASC
         LIMIT $1`,
		tableOutbox,
	)
	rows, err := w.db.QueryContext(ctx, selectQuery, w.cfg.BatchSize)
	if err != nil {
		log.Printf("outbox worker: failed to fetch pending events: %v", err)
		w.outbox.markDBProblem()
		return
	}
	defer rows.Close()

	type record struct {
		id       string
		eventKey string
		payload  []byte
		attempts int
	}
	var records []record
	for rows.Next() {
		var r record
		if err := rows.Scan(&r.id, &r.eventKey, &r.payload, &r.attempts); err != nil {
			log.Printf("outbox worker: scan failed: %v", err)
			continue
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		log.Printf("outbox worker: rows iteration failed: %v", err)
		w.outbox.markDBProblem()
		return
	}

	if len(records) == 0 {
		return
	}
	if w.cfg.EnableConsoleLogging {
		log.Printf("outbox worker: processing %d records", len(records))
	}

	sender := w.outbox.GetSender()
	if sender == nil {
		log.Printf("outbox worker: Kafka sender is not available, attempting to recreate")
		w.outbox.tryCreateSender()
		sender = w.outbox.GetSender()
		if sender == nil {
			log.Printf("outbox worker: cannot send, Kafka still unavailable")
			return
		}
	}

	encoder := w.outbox.encoder
	for _, rec := range records {
		encodedKey, encodedValue, err := encoder.Encode(rec.eventKey, rec.payload)
		if err != nil {
			w.markEncodeFailure(ctx, tableOutbox, rec.id, rec.attempts, err)
			continue
		}
		if err := sender.Send(ctx, rec.eventKey, encodedKey, encodedValue); err != nil {
			log.Printf("outbox worker: send to Kafka failed for id=%s, key=%s: %v", rec.id, rec.eventKey, err)
			w.outbox.MarkSenderBroken()
			return
		}
		updateQuery := fmt.Sprintf("UPDATE %s SET published_at = NOW() WHERE id = $1", tableOutbox)
		if _, err := w.db.ExecContext(ctx, updateQuery, rec.id); err != nil {
			log.Printf("outbox worker: mark published failed for id=%s: %v", rec.id, err)
			w.outbox.markDBProblem()
		} else if w.cfg.EnableConsoleLogging {
			log.Printf("outbox worker: successfully published and marked id=%s, key=%s", rec.id, rec.eventKey)
		}
	}
}

// markEncodeFailure инкрементирует attempts и при достижении порога помечает
// запись failed_at, чтобы отравленное событие больше не выбиралось.
func (w *Worker) markEncodeFailure(ctx context.Context, tbl, id string, prevAttempts int, encErr error) {
	attempts := prevAttempts + 1
	if attempts >= w.cfg.MaxEncodeAttempts {
		upd := fmt.Sprintf("UPDATE %s SET attempts=$1, failed_at=NOW(), last_error=$2 WHERE id=$3", tbl)
		if _, err := w.db.ExecContext(ctx, upd, attempts, encErr.Error(), id); err != nil {
			log.Printf("outbox worker: failed to mark encode failure for id=%s: %v", id, err)
			w.outbox.markDBProblem()
			return
		}
		log.Printf("outbox worker: encode permanently failed id=%s after %d attempts: %v", id, attempts, encErr)
		return
	}
	upd := fmt.Sprintf("UPDATE %s SET attempts=$1, last_error=$2 WHERE id=$3", tbl)
	if _, err := w.db.ExecContext(ctx, upd, attempts, encErr.Error(), id); err != nil {
		log.Printf("outbox worker: failed to record encode attempt for id=%s: %v", id, err)
		w.outbox.markDBProblem()
		return
	}
	log.Printf("outbox worker: encode failed id=%s attempt %d: %v", id, attempts, encErr)
}

// checkBacklog при превышении порога пишет WARN-лог о числе неотправленных
// событий. Троттлится по BacklogCheckInterval, чтобы не нагружать БД.
func (w *Worker) checkBacklog(ctx context.Context, tbl string) {
	if w.cfg.BacklogWarnThreshold <= 0 {
		return
	}
	if time.Since(w.lastBacklogTime) < w.cfg.BacklogCheckInterval {
		return
	}
	w.lastBacklogTime = time.Now()

	var n int
	var oldest sql.NullTime
	q := fmt.Sprintf("SELECT COUNT(*), MIN(created_at) FROM %s WHERE published_at IS NULL AND failed_at IS NULL", tbl)
	if err := w.db.QueryRowContext(ctx, q).Scan(&n, &oldest); err != nil {
		log.Printf("outbox worker: backlog check failed: %v", err)
		return
	}
	if n >= w.cfg.BacklogWarnThreshold {
		age := time.Duration(0)
		if oldest.Valid {
			age = time.Since(oldest.Time)
		}
		log.Printf("WARN outbox worker: backlog %d unpublished events, oldest age %s", n, age.Round(time.Second))
	}
}

// cleanup удаляет отправленные строки старше CleanupRetention.
func (w *Worker) cleanup() {
	tbl := fullTableName(w.cfg.Schema, "outbox")
	q := fmt.Sprintf("DELETE FROM %s WHERE published_at IS NOT NULL AND published_at < NOW() - $1::interval", tbl)
	interval := fmt.Sprintf("%d seconds", int(w.cfg.CleanupRetention.Seconds()))
	res, err := w.db.ExecContext(context.Background(), q, interval)
	if err != nil {
		log.Printf("outbox worker: cleanup failed: %v", err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 && w.cfg.EnableConsoleLogging {
		log.Printf("outbox worker: cleanup removed %d published rows", n)
	}
}

func (w *Worker) Stop() {
	close(w.stopCh)
}
