package outbox

import (
	"testing"
	"time"
)

func TestConfigValidateAndDefault(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		wantErr    bool
		assertFunc func(t *testing.T, c Config)
	}{
		{
			name: "empty schema defaults to public",
			cfg:  Config{},
			assertFunc: func(t *testing.T, c Config) {
				if c.Schema != "public" {
					t.Fatalf("Schema = %q, want public", c.Schema)
				}
			},
		},
		{
			name:    "negative batch size is error",
			cfg:     Config{BatchSize: -1},
			wantErr: true,
		},
		{
			name: "zero batch size defaults to 100",
			cfg:  Config{BatchSize: 0},
			assertFunc: func(t *testing.T, c Config) {
				if c.BatchSize != defaultBatchSize {
					t.Fatalf("BatchSize = %d, want %d", c.BatchSize, defaultBatchSize)
				}
			},
		},
		{
			name: "positive batch size kept",
			cfg:  Config{BatchSize: 50},
			assertFunc: func(t *testing.T, c Config) {
				if c.BatchSize != 50 {
					t.Fatalf("BatchSize = %d, want 50", c.BatchSize)
				}
			},
		},
		{
			name: "zero max encode attempts defaults",
			cfg:  Config{},
			assertFunc: func(t *testing.T, c Config) {
				if c.MaxEncodeAttempts != defaultMaxEncodeAttempts {
					t.Fatalf("MaxEncodeAttempts = %d, want %d", c.MaxEncodeAttempts, defaultMaxEncodeAttempts)
				}
			},
		},
		{
			name:    "negative backlog threshold is error",
			cfg:     Config{BacklogWarnThreshold: -1},
			wantErr: true,
		},
		{
			name: "backlog check interval defaults",
			cfg:  Config{},
			assertFunc: func(t *testing.T, c Config) {
				if c.BacklogCheckInterval != defaultBacklogCheckInterval {
					t.Fatalf("BacklogCheckInterval = %s, want %s", c.BacklogCheckInterval, defaultBacklogCheckInterval)
				}
			},
		},
		{
			name:    "send enabled binary without brokers is error",
			cfg:     Config{SendEnabled: true, Mode: "binary", KafkaTopic: "t"},
			wantErr: true,
		},
		{
			name:    "send enabled binary without topic is error",
			cfg:     Config{SendEnabled: true, Mode: "binary", KafkaBrokers: []string{"k:9092"}},
			wantErr: true,
		},
		{
			name: "send enabled binary valid",
			cfg:  Config{SendEnabled: true, Mode: "binary", KafkaBrokers: []string{"k:9092"}, KafkaTopic: "t"},
		},
		{
			name:    "send enabled schema-registry without rest url is error",
			cfg:     Config{SendEnabled: true, Mode: "schema-registry", KafkaTopic: "t"},
			wantErr: true,
		},
		{
			name:    "send enabled unknown mode is error",
			cfg:     Config{SendEnabled: true, Mode: "weird"},
			wantErr: true,
		},
		{
			name: "send disabled does not require brokers",
			cfg:  Config{SendEnabled: false, Mode: ""},
		},
		{
			name:    "cleanup enabled without retention is error",
			cfg:     Config{CleanupEnabled: true},
			wantErr: true,
		},
		{
			name: "cleanup enabled interval defaults",
			cfg:  Config{CleanupEnabled: true, CleanupRetention: time.Hour},
			assertFunc: func(t *testing.T, c Config) {
				if c.CleanupInterval != defaultCleanupInterval {
					t.Fatalf("CleanupInterval = %s, want %s", c.CleanupInterval, defaultCleanupInterval)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			err := cfg.validateAndDefault()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.assertFunc != nil {
				tt.assertFunc(t, cfg)
			}
		})
	}
}
