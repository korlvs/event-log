package outbox

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

func quoteIdentifier(name string) string {
	return `"` + name + `"`
}

func fullTableName(schema, table string) string {
	if schema == "" {
		return quoteIdentifier(table)
	}
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

func ensureMigrationsTable(db *sql.DB, schema string) error {
	tableName := fullTableName(schema, "outbox_schema_migrations")
	query := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        );
    `, tableName)
	_, err := db.Exec(query)
	return err
}

func getAppliedVersions(db *sql.DB, schema string) (map[int]bool, error) {
	tableName := fullTableName(schema, "outbox_schema_migrations")
	query := fmt.Sprintf("SELECT version FROM %s ORDER BY version", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

func applyMigration(db *sql.DB, schema string, version int, sqlScript string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Выполняем SQL миграции (сами скрипты не содержат схемы, полагаемся на search_path)
	if _, err := tx.Exec(sqlScript); err != nil {
		return fmt.Errorf("migration %d: %w", version, err)
	}

	tableName := fullTableName(schema, "outbox_schema_migrations")
	insertQuery := fmt.Sprintf("INSERT INTO %s (version) VALUES ($1)", tableName)
	if _, err := tx.Exec(insertQuery, version); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", version, err)
	}
	return tx.Commit()
}

// RunMigrations применяет все новые миграции из встроенных файлов.
func RunMigrations(db *sql.DB, schema string) error {
	if err := ensureMigrationsTable(db, schema); err != nil {
		return err
	}
	applied, err := getAppliedVersions(db, schema)
	if err != nil {
		return err
	}

	entries, err := MigrationsFS.ReadDir("migrations")
	if err != nil {
		return err
	}

	type migration struct {
		version int
		name    string
		content string
	}
	var migrations []migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		var version int
		if n, _ := fmt.Sscanf(entry.Name(), "%d_", &version); n != 1 {
			continue
		}
		content, err := MigrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		migrations = append(migrations, migration{
			version: version,
			name:    entry.Name(),
			content: string(content),
		})
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		if err := applyMigration(db, schema, m.version, m.content); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", m.name, err)
		}
	}
	return nil
}
