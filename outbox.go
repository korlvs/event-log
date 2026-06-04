package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	eventpb "github.com/korlvs/event-contract"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	globalInstance *Outbox
	once           sync.Once
)

type Outbox struct {
	db             *sql.DB
	cfg            Config
	sender         Sender
	encoder        Encoder
	worker         *Worker
	mu             sync.RWMutex
	senderBroken   bool
	dbProblem      bool
	dbMu           sync.Mutex
	dbRecoveryStop chan struct{}
}

func Init(db *sql.DB, cfg Config) error {
	var err error
	once.Do(func() {
		var o *Outbox
		o, err = initOutbox(db, cfg)
		if err != nil {
			return
		}
		globalInstance = o
	})
	return err
}

// initOutbox конструирует Outbox и выполняет setup без обращения к sync.Once.
// Вынесено отдельно для тестируемости (export_test.go обходит синглтон).
func initOutbox(db *sql.DB, cfg Config) (*Outbox, error) {
	if err := cfg.validateAndDefault(); err != nil {
		return nil, err
	}
	o := &Outbox{db: db, cfg: cfg, senderBroken: true, dbProblem: false}
	if err := o.setup(); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *Outbox) setup() error {
	// Энкодер строится при заданном Mode (дёшево, без сети); при пустом Mode
	// и выключенной доставке encoder=nil, worker всё равно не стартует.
	switch o.cfg.Mode {
	case "schema-registry":
		o.encoder = NewSchemaRegistryEncoder(o.cfg.SchemaIDKey, o.cfg.SchemaIDValue)
	case "binary":
		o.encoder = NewBinaryEncoder()
	}

	// Прогоняем миграции при старте, чтобы первое событие не терялось.
	if err := o.ensureTable(); err != nil {
		return fmt.Errorf("outbox: failed to ensure schema on init: %w", err)
	}

	// Доставка стартует только при SendEnabled — запись в outbox работает независимо.
	if o.cfg.SendEnabled {
		o.tryCreateSender()

		o.dbRecoveryStop = make(chan struct{})
		go o.dbRecoveryLoop()

		o.worker = NewWorker(o.db, o, o.cfg)
		go o.worker.Start()
		if o.cfg.EnableConsoleLogging {
			log.Println("outbox: worker started")
		}
	}
	return nil
}

func (o *Outbox) tryCreateSender() {
	o.mu.Lock()
	defer o.mu.Unlock()
	var sender Sender
	var err error
	switch o.cfg.Mode {
	case "schema-registry":
		sender, err = NewRestSender(o.cfg)
	case "binary":
		sender, err = NewSaramaSender(
			o.cfg.KafkaBrokers,
			o.cfg.KafkaTopic,
			o.cfg.KafkaUsername,
			o.cfg.KafkaPassword,
			o.cfg.KafkaTLSEnabled,
			o.cfg.KafkaTLSInsecureSkipVerify,
			o.cfg.KafkaSaslMechanism,
		)
	}
	if err != nil {
		log.Printf("outbox: failed to create Kafka sender: %v (will retry later)", err)
		o.sender = nil
		o.senderBroken = true
		return
	}
	o.sender = sender
	o.senderBroken = false
	if o.cfg.EnableConsoleLogging {
		log.Println("outbox: Kafka sender created successfully")
	}
}

func (o *Outbox) GetSender() Sender {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.senderBroken {
		return nil
	}
	return o.sender
}

func (o *Outbox) MarkSenderBroken() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.senderBroken = true
	if o.sender != nil {
		if closer, ok := o.sender.(interface{ Close() error }); ok {
			closer.Close()
		}
		o.sender = nil
	}
}

// buildInsert собирает INSERT в таблицу outbox с учётом схемы из конфига и StoreJSON.
func (o *Outbox) buildInsert(key string, protoBytes, jsonEvent []byte) (string, []interface{}) {
	tbl := fullTableName(o.cfg.Schema, "outbox")
	if o.cfg.StoreJSON {
		return fmt.Sprintf("INSERT INTO %s (event_key, payload, payload_json) VALUES ($1, $2, $3)", tbl),
			[]interface{}{key, protoBytes, jsonEvent}
	}
	return fmt.Sprintf("INSERT INTO %s (event_key, payload) VALUES ($1, $2)", tbl),
		[]interface{}{key, protoBytes}
}

func (o *Outbox) ensureTable() error {
	o.dbMu.Lock()
	defer o.dbMu.Unlock()

	var exists bool
	err := o.db.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'outbox')").Scan(&exists)
	if err != nil {
		log.Printf("outbox: failed to check table existence: %v", err)
		return err
	}
	if exists {
		o.dbProblem = false
		return nil
	}

	log.Println("outbox: table 'outbox' missing, applying migrations")
	if err := RunMigrations(o.db, o.cfg.Schema); err != nil {
		log.Printf("outbox: migrations failed: %v", err)
		return err
	}
	if o.cfg.EnableConsoleLogging {
		log.Println("outbox: table created successfully")
	}
	o.dbProblem = false
	return nil
}

// markDBProblem потокобезопасно взводит флаг проблемы с БД.
func (o *Outbox) markDBProblem() {
	o.dbMu.Lock()
	o.dbProblem = true
	o.dbMu.Unlock()
}

// clearDBProblem потокобезопасно сбрасывает флаг проблемы с БД.
func (o *Outbox) clearDBProblem() {
	o.dbMu.Lock()
	wasProblem := o.dbProblem
	o.dbProblem = false
	o.dbMu.Unlock()
	if wasProblem {
		log.Println("outbox: DB recovered (successful insert)")
	}
}

// hasDBProblem потокобезопасно читает флаг проблемы с БД.
func (o *Outbox) hasDBProblem() bool {
	o.dbMu.Lock()
	defer o.dbMu.Unlock()
	return o.dbProblem
}

// handleEncodeFailure логирует и возвращает ошибку сериализации proto при записи.
// Запись в этот момент ещё не попала в БД, поэтому маркировать в outbox нечего.
func (o *Outbox) handleEncodeFailure(_ context.Context, key string, jsonEvent []byte, err error) error {
	log.Printf("outbox: failed to marshal proto for key=%s, event=%s: %v", key, string(jsonEvent), err)
	return err
}

// PublishEventWithExecutor пишет событие через произвольный executor,
// что позволяет участвовать во внешней транзакции хоста (pgx, database/sql, мок).
func PublishEventWithExecutor(ctx context.Context, exec Executor, key string, event *eventpb.Event) error {
	if globalInstance == nil {
		log.Printf("outbox: not initialized, cannot publish event key=%s", key)
		return ErrNotInitialized
	}

	enrichEvent(ctx, event)

	jsonEvent, _ := protojson.Marshal(event)

	protoBytes, err := proto.Marshal(event)
	if err != nil {
		return globalInstance.handleEncodeFailure(ctx, key, jsonEvent, err)
	}

	query, args := globalInstance.buildInsert(key, protoBytes, jsonEvent)
	if err := exec.Exec(ctx, query, args...); err != nil {
		log.Printf("outbox: insert failed via executor for key=%s, event=%s: %v", key, string(jsonEvent), err)
		globalInstance.markDBProblem()
		return err
	}
	globalInstance.clearDBProblem()

	if globalInstance.cfg.EnableConsoleLogging {
		log.Printf("outbox: event published successfully, key=%s, event=%s", key, string(jsonEvent))
	}
	return nil
}

// PublishEvent публикует событие без транзакции (autocommit через пул библиотеки).
func PublishEvent(ctx context.Context, key string, event *eventpb.Event) error {
	if globalInstance == nil {
		log.Printf("outbox: not initialized, cannot publish event key=%s", key)
		return ErrNotInitialized
	}
	exec := ExecutorFunc(func(c context.Context, q string, a ...any) error {
		_, err := globalInstance.db.ExecContext(c, q, a...)
		return err
	})
	return PublishEventWithExecutor(ctx, exec, key, event)
}

// PublishEventWithTx публикует событие в рамках переданной database/sql транзакции.
func PublishEventWithTx(ctx context.Context, tx *sql.Tx, key string, event *eventpb.Event) error {
	if globalInstance == nil {
		log.Printf("outbox: not initialized, cannot publish event key=%s", key)
		return ErrNotInitialized
	}
	exec := ExecutorFunc(func(c context.Context, q string, a ...any) error {
		_, err := tx.ExecContext(c, q, a...)
		return err
	})
	return PublishEventWithExecutor(ctx, exec, key, event)
}

// enrichEvent заполняет недостающие поля события из контекста и конфигурации
func enrichEvent(ctx context.Context, event *eventpb.Event) {
	if event.SchemaVersion == "" {
		event.SchemaVersion = "1.0"
	}
	if event.Timestamp == nil {
		event.Timestamp = timestamppb.Now()
	}
	if event.Context == nil {
		event.Context = &eventpb.RequestContext{}
	}
	rc := event.Context

	meta := RequestMetadataFromContext(ctx)
	if meta != nil {
		if rc.ClientIp == "" {
			rc.ClientIp = meta.ClientIP
		}
		if rc.CorrelationId == "" {
			rc.CorrelationId = meta.CorrelationID
		}
		if rc.UserAgent == "" {
			rc.UserAgent = meta.UserAgent
		}
	}
	if rc.SourceService == "" && globalInstance.cfg.ServiceName != "" {
		rc.SourceService = globalInstance.cfg.ServiceName
	}
	if rc.Environment == "" && globalInstance.cfg.Environment != "" {
		rc.Environment = globalInstance.cfg.Environment
	}

	if event.Actor == nil {
		event.Actor = &eventpb.Actor{}
	}
	if meta != nil {
		if event.Actor.Id == "" {
			event.Actor.Id = meta.UserID
		}
		if event.Actor.DisplayName == "" {
			event.Actor.DisplayName = meta.UserEmail
		}
	}
	if event.Actor.Type == "" {
		if event.Actor.Id != "" {
			event.Actor.Type = "user"
		} else {
			event.Actor.Type = "anonymous"
		}
	}

	if event.Details == nil {
		event.Details = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	if globalInstance.cfg.ServiceVersion != "" {
		event.Details.Fields["service_version"] = structpb.NewStringValue(globalInstance.cfg.ServiceVersion)
	}
	if _, ok := event.Details.Fields["environment"]; !ok && globalInstance.cfg.Environment != "" {
		event.Details.Fields["environment"] = structpb.NewStringValue(globalInstance.cfg.Environment)
	}
}

// dbRecoveryLoop фоновая горутина, восстанавливающая таблицу outbox при проблемах
func (o *Outbox) dbRecoveryLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if o.hasDBProblem() {
				// Проверяем существование таблицы
				var exists bool
				err := o.db.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'outbox')").Scan(&exists)
				if err != nil {
					log.Printf("outbox: DB recovery check failed: %v", err)
					continue
				}
				if !exists {
					log.Println("outbox: attempting to create missing outbox table via migrations")
					if err := RunMigrations(o.db, o.cfg.Schema); err != nil {
						log.Printf("outbox: DB recovery migrations failed: %v", err)
					} else {
						o.clearDBProblem()
						log.Println("outbox: DB recovery successful")
					}
				} else {
					// Таблица существует, сбрасываем флаг
					o.clearDBProblem()
					log.Println("outbox: DB recovery: table exists, flag cleared")
				}
			}
		case <-o.dbRecoveryStop:
			return
		}
	}
}

func Shutdown() {
	if globalInstance != nil {
		if globalInstance.worker != nil {
			globalInstance.worker.Stop()
		}
		if globalInstance.dbRecoveryStop != nil {
			close(globalInstance.dbRecoveryStop)
		}
		log.Println("outbox: stopped")
	}
}
