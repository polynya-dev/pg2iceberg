-- SETUP --
CREATE TABLE e2e_partition_hour (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE e2e_partition_hour REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_partition_hour FOR TABLE e2e_partition_hour;
-- DATA --
INSERT INTO e2e_partition_hour (id, name, created_at) VALUES
    (1, 'alice',   '2025-01-15 10:00:00+00'),
    (2, 'bob',     '2025-01-15 10:30:00+00'),
    (3, 'charlie', '2025-01-15 14:00:00+00'),
    (4, 'dave',    '2025-01-15 14:45:00+00'),
    (5, 'eve',     '2025-01-15 20:00:00+00');
