package outbox

import (
	"fmt"
	"time"
)

const (
	defaultBatchSize            = 100
	defaultMaxEncodeAttempts    = 5
	defaultCleanupInterval      = time.Hour
	defaultBacklogCheckInterval = time.Minute
	defaultDeliverInterval      = 5 * time.Second
)

type Config struct {
	Mode          string // "binary" или "schema-registry"
	KafkaTopic    string
	KafkaBrokers  []string // для binary
	KafkaRestURL  string   // для schema-registry
	KafkaUsername string
	KafkaPassword string
	SchemaIDKey   int
	SchemaIDValue int
	Workers       int
	BatchSize     int

	// Новые поля для обогащения событий
	ServiceName    string // имя микросервиса (source_service)
	ServiceVersion string // версия сервиса (добавляется в details)
	Environment    string // production / staging / development

	Schema               string
	StoreJSON            bool
	EnableConsoleLogging bool

	KafkaTLSEnabled            bool
	KafkaTLSInsecureSkipVerify bool
	KafkaSaslMechanism         string

	// SendEnabled включает доставку (worker + sender + dbRecoveryLoop).
	// При false запись в outbox работает независимо, события копятся в таблице.
	SendEnabled bool

	// CleanupEnabled включает периодическую очистку отправленных строк.
	CleanupEnabled   bool
	CleanupRetention time.Duration // хранить published строки не дольше этого срока
	CleanupInterval  time.Duration // как часто запускать очистку (дефолт 1h)

	// MaxEncodeAttempts — порог попыток кодирования, после которого запись
	// помечается failed_at и больше не выбирается worker'ом (0 → дефолт 5).
	MaxEncodeAttempts int

	// BacklogWarnThreshold — порог числа неотправленных событий для WARN-лога
	// (0 → проверка выключена).
	BacklogWarnThreshold int
	BacklogCheckInterval time.Duration // как часто проверять backlog (дефолт 1m)
}

// validateAndDefault проверяет конфиг и проставляет безопасные дефолты.
// Вызывается в Init до setup.
func (c *Config) validateAndDefault() error {
	if c.Schema == "" {
		c.Schema = "public"
	}
	if c.BatchSize < 0 {
		return fmt.Errorf("outbox: BatchSize must be >= 0, got %d", c.BatchSize)
	}
	if c.BatchSize == 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.MaxEncodeAttempts <= 0 {
		c.MaxEncodeAttempts = defaultMaxEncodeAttempts
	}
	if c.BacklogWarnThreshold < 0 {
		return fmt.Errorf("outbox: BacklogWarnThreshold must be >= 0, got %d", c.BacklogWarnThreshold)
	}
	if c.BacklogCheckInterval <= 0 {
		c.BacklogCheckInterval = defaultBacklogCheckInterval
	}

	if c.SendEnabled {
		switch c.Mode {
		case "binary":
			if len(c.KafkaBrokers) == 0 {
				return fmt.Errorf("outbox: binary mode requires KafkaBrokers when send is enabled")
			}
			if c.KafkaTopic == "" {
				return fmt.Errorf("outbox: KafkaTopic is required when send is enabled")
			}
		case "schema-registry":
			if c.KafkaRestURL == "" {
				return fmt.Errorf("outbox: schema-registry mode requires KafkaRestURL when send is enabled")
			}
			if c.KafkaTopic == "" {
				return fmt.Errorf("outbox: KafkaTopic is required when send is enabled")
			}
		default:
			return fmt.Errorf("outbox: unknown mode: %q", c.Mode)
		}
	}

	if c.CleanupEnabled {
		if c.CleanupRetention <= 0 {
			return fmt.Errorf("outbox: CleanupRetention must be > 0 when cleanup is enabled")
		}
		if c.CleanupInterval <= 0 {
			c.CleanupInterval = defaultCleanupInterval
		}
	}
	return nil
}

// deliverInterval возвращает интервал тикера доставки worker'а.
func (c *Config) deliverInterval() time.Duration {
	return defaultDeliverInterval
}
