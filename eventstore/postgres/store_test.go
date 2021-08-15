package postgres_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/get-eventually/go-eventually/eventstore"
	"github.com/get-eventually/go-eventually/eventstore/postgres"
	"github.com/get-eventually/go-eventually/extension/zaplogger"
	"github.com/get-eventually/go-eventually/internal"
)

const defaultPostgresURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

func obtainEventStore(t *testing.T) postgres.EventStore {
	if testing.Short() {
		t.SkipNow()
	}

	url, ok := os.LookupEnv("DATABASE_URL")
	if !ok {
		url = defaultPostgresURL
	}

	require.NoError(t, postgres.RunMigrations(url))

	db, err := sql.Open("postgres", url)
	require.NoError(t, err)

	registry, err := eventstore.NewRegistry(json.Unmarshal)
	require.NoError(t, err)
	require.NoError(t, registry.Register(internal.IntPayload(0)))

	return postgres.EventStore{
		DB:       db,
		Registry: registry,
		Logger:   zaplogger.Wrap(zap.L()),
	}
}

func TestStoreSuite(t *testing.T) {
	store := obtainEventStore(t)
	defer func() { assert.NoError(t, store.DB.Close()) }()

	suite.Run(t, eventstore.NewStoreSuite(func() eventstore.Store {
		tx, err := store.DB.Begin()
		require.NoError(t, err)

		// Reset checkpoints for subscriptions.
		_, err = tx.Exec("DELETE FROM subscriptions_checkpoints")
		require.NoError(t, err)

		// Reset committed events and streams.
		_, err = tx.Exec("DELETE FROM streams")
		require.NoError(t, err)

		// Reset the global sequence number to 1.
		_, err = tx.Exec("ALTER SEQUENCE events_global_sequence_number_seq RESTART WITH 1")
		require.NoError(t, err)

		require.NoError(t, tx.Commit())

		return store
	}))
}
