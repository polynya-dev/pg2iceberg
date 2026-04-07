-- SETUP --
CREATE TABLE e2e_partition_day (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE e2e_partition_day REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_partition_day (id, name, created_at) VALUES
    (1, 'alice',   '2025-01-15 10:00:00+00'),
    (2, 'bob',     '2025-01-15 14:30:00+00'),
    (3, 'charlie', '2025-02-20 09:00:00+00'),
    (4, 'dave',    '2025-02-20 18:45:00+00'),
    (5, 'eve',     '2025-03-10 12:00:00+00');
