-- Deliberately small schema for a clear end-to-end demo. Two related tables
-- with primary keys and a mix of INSERT / UPDATE / DELETE traffic.
--
-- The same file is applied to both the blue and green clusters so their
-- table layouts match byte-for-byte before logical replication starts
-- (PG's logical replication does not ship DDL, only DML).

-- pg2iceberg creates this schema + table itself on first startup, but we
-- pre-create them here so both clusters have matching structure before the
-- bluegreen subscription is set up.
CREATE SCHEMA IF NOT EXISTS _pg2iceberg;

CREATE TABLE IF NOT EXISTS _pg2iceberg.markers (
    uuid       TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
    id         SERIAL PRIMARY KEY,
    email      TEXT    NOT NULL UNIQUE,
    balance    INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transfers (
    id         SERIAL PRIMARY KEY,
    from_acct  INTEGER NOT NULL REFERENCES accounts(id),
    to_acct    INTEGER NOT NULL REFERENCES accounts(id),
    amount     INTEGER NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL so updates and deletes carry the full row, which
-- simplifies comparing rows across pg2iceberg instances.
ALTER TABLE accounts  REPLICA IDENTITY FULL;
ALTER TABLE transfers REPLICA IDENTITY FULL;
