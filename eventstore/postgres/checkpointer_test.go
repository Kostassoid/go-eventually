package postgres_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/get-eventually/go-eventually/eventstore/postgres"
)

func TestCheckpointer(t *testing.T) {
	store := obtainEventStore(t)
	defer func() { assert.NoError(t, store.DB.Close()) }()

	ctx := context.Background()

	checkpointer := postgres.Checkpointer{
		DB:     store.DB,
		Logger: store.Logger,
	}

	const subscriptionName = "test-subscription"

	seqNum, err := checkpointer.Read(ctx, subscriptionName)
	assert.NoError(t, err)
	assert.Zero(t, seqNum)

	newSeqNum := int64(1200)
	err = checkpointer.Write(ctx, subscriptionName, newSeqNum)
	assert.NoError(t, err)

	seqNum, err = checkpointer.Read(ctx, subscriptionName)
	assert.NoError(t, err)
	assert.Equal(t, newSeqNum, seqNum)
}
