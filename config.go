package outbox

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
}
