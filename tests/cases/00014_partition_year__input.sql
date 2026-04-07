-- SETUP --
CREATE TABLE e2e_partition_year (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE e2e_partition_year REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_partition_year (id, name, created_at) VALUES
    (1, 'alice',   '2023-06-15 10:00:00+00'),
    (2, 'bob',     '2023-11-20 14:30:00+00'),
    (3, 'charlie', '2024-03-10 09:00:00+00'),
    (4, 'dave',    '2024-08-20 18:45:00+00'),
    (5, 'eve',     '2025-01-10 12:00:00+00');
