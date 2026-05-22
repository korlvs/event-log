package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

type Worker struct {
	db     *sql.DB
	outbox *Outbox
	cfg    Config
	stopCh chan struct{}
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
		log.Println("outbox worker: started, interval=5s")
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.processBatch()
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

	if w.outbox.dbProblem {
		if err := w.outbox.ensureTable(); err != nil {
			log.Printf("outbox worker: DB still unavailable: %v", err)
			return
		}
	}

	tableOutbox := fullTableName(w.cfg.Schema, "outbox")
	selectQuery := fmt.Sprintf(
		`SELECT id, event_key, payload FROM %s
         WHERE published_at IS NULL
         ORDER BY created_at ASC
         LIMIT $1`,
		tableOutbox,
	)
	rows, err := w.db.QueryContext(ctx, selectQuery, w.cfg.BatchSize)
	if err != nil {
		log.Printf("outbox worker: failed to fetch pending events: %v", err)
		w.outbox.dbProblem = true
		return
	}
	defer rows.Close()

	type record struct {
		id       string
		eventKey string
		payload  []byte
	}
	var records []record
	for rows.Next() {
		var r record
		if err := rows.Scan(&r.id, &r.eventKey, &r.payload); err != nil {
			log.Printf("outbox worker: scan error: %v", err)
			continue
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		log.Printf("outbox worker: rows error: %v", err)
		w.outbox.dbProblem = true
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
		log.Printf("outbox worker: Kafka sender is not available, attempting to recreate...")
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
			log.Printf("outbox worker: encode failed for id=%s: %v", rec.id, err)
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
			w.outbox.dbProblem = true
		} else if w.cfg.EnableConsoleLogging {
			log.Printf("outbox worker: successfully published and marked id=%s, key=%s", rec.id, rec.eventKey)
		}
	}
}

func (w *Worker) Stop() {
	close(w.stopCh)
}
