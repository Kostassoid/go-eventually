-- Add this extension to create a random UUID v4 for consumer group leases.
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE consumer_groups (
    consumer_group_name TEXT    PRIMARY KEY,
    size                INTEGER NOT NULL     DEFAULT 1 CHECK(size > 0)
);

CREATE TABLE consumer_group_leases (
    lease_id            UUID      PRIMARY KEY DEFAULT uuid_generate_v4(),
    consumer_group_name TEXT      NOT NULL    REFERENCES consumer_groups(consumer_group_name) ON DELETE CASCADE,
    member              INTEGER   NOT NULL    CHECK(member >= 0),
    leased_at           TIMESTAMP NOT NULL    DEFAULT NOW(),
    expires_at          TIMESTAMP NOT NULL,
    last_action_at      TIMESTAMP NOT NULL    DEFAULT NOW()
);

-- Create a unique index for the global_sequence_number, since it is not allowed
-- to have multiple events with the same sequence number.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ON events (global_sequence_number);

-- This index should help speed up lookups when streaming events by their stream type.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ON events (stream_type, global_sequence_number);

-- Use this function to get a "lease" as a member of a specific Consumer Group,
-- to be able to stream messages using stream_by_type() or stream_all()
-- using multiple consumers.
CREATE OR REPLACE FUNCTION announce_consumer_group_member (
    _consumer_group_name TEXT
) RETURNS SETOF consumer_group_leases
AS $$
DECLARE
    consumer_group_size INTEGER;
    number_of_leases    INTEGER;
BEGIN

    SELECT cg.size INTO consumer_group_size
    FROM consumer_groups cg
    WHERE cg.consumer_group_name = _consumer_group_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'ERR-0001: consumer group was not found, are you sure it has been set up?';
    END IF;

    SELECT count(*) INTO number_of_leases
    FROM consumer_group_leases cgl
    WHERE cgl.consumer_group_name = _consumer_group_name AND expires_at > NOW();

    IF number_of_leases >= consumer_group_size THEN
        RAISE EXCEPTION 'ERR-0002: all possible leases have been already reserved';
    END IF;

    RETURN QUERY
        INSERT INTO consumer_group_leases (consumer_group_name, member, expires_at)
        VALUES (_consumer_group_name, number_of_leases, NOW() + INTERVAL '1' HOUR)
        RETURNING *;

END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION hash_64 (
  value VARCHAR
)
RETURNS BIGINT
AS $$
DECLARE
  _hash BIGINT;
BEGIN

  SELECT left('x' || md5(hash_64.value), 17)::BIT(64)::BIGINT INTO _hash;
  RETURN _hash;

END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION _stream_events (
    from_sequence_number    INTEGER,
    batch_size              INTEGER,
    consumer_group_lease_id UUID,
    conditions              TEXT DEFAULT NULL
) RETURNS SETOF events
AS $$
DECLARE
    lease_expiry          TIMESTAMP;
    consumer_group_member INTEGER;
    consumer_group_size   INTEGER;
    _command              TEXT;
BEGIN

    SELECT cgl.expires_at, cgl.member, cg.size
    INTO lease_expiry, consumer_group_member, consumer_group_size
    FROM consumer_group_leases cgl
    LEFT JOIN consumer_groups cg ON cg.consumer_group_name = cgl.consumer_group_name
    WHERE cgl.lease_id = consumer_group_lease_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION '0001: consumer group lease was not found, use announce_consumer_group_member() to register a lease';
    END IF;

    IF lease_expiry < NOW() THEN
        RAISE EXCEPTION '0002: consumer group lease has expired';
    END IF;

    UPDATE consumer_group_leases
    SET
        last_action_at = NOW(),
        expires_at     = NOW() + INTERVAL '1' HOUR
    WHERE
        lease_id = consumer_group_lease_id;

    _command = 'SELECT *
                FROM events e
                WHERE e.global_sequence_number > $1
                AND MOD(@hash_64(e.stream_id), $2) = $3';

    IF conditions IS NOT NULL THEN
        _command = _command || '
            AND (%s)';
        _command = format(_command, conditions);
    END IF;

    _command = _command || '
        ORDER BY e.global_sequence_number';

    IF batch_size > 0 THEN
        _command = _command || '
            LIMIT $4';
    END IF;

    RETURN QUERY EXECUTE _command USING
   		from_sequence_number,
   		consumer_group_size,
		consumer_group_member,
		batch_size;

END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION stream_by_type (
    _stream_type             TEXT,
    from_sequence_number    INTEGER,
    batch_size              INTEGER,
    consumer_group_lease_id UUID
) RETURNS SETOF events
AS $$
BEGIN

    RETURN QUERY SELECT * FROM _stream_events (
        from_sequence_number,
        batch_size,
        consumer_group_lease_id,
        format('e.stream_type = ''%s''', _stream_type)
    );

END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION stream_all (
    from_sequence_number    INTEGER,
    batch_size              INTEGER,
    consumer_group_lease_id UUID
) RETURNS SETOF events
AS $$
BEGIN

    RETURN QUERY SELECT * FROM _stream_events (
        from_sequence_number,
        batch_size,
        consumer_group_lease_id
    );

END;
$$ LANGUAGE PLPGSQL;
