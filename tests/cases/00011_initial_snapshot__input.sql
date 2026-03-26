-- SETUP --
CREATE TABLE e2e_initial_snapshot (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);
ALTER TABLE e2e_initial_snapshot REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_initial_snapshot FOR TABLE e2e_initial_snapshot;

-- Pre-existing rows: inserted BEFORE pg2iceberg starts.
-- These should be captured by initial snapshot.
INSERT INTO e2e_initial_snapshot (id, name, amount) VALUES
    (1, 'alice', 100.50),
    (2, 'bob', 200.75),
    (3, 'charlie', 300.00);

-- DATA --
-- Row inserted AFTER pg2iceberg starts (captured via logical replication).
INSERT INTO e2e_initial_snapshot (id, name, amount) VALUES
    (4, 'diana', 400.25);
