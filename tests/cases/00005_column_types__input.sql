-- SETUP --
CREATE TABLE e2e_column_types (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER NOT NULL,
    balance NUMERIC(12,2),
    is_active BOOLEAN NOT NULL DEFAULT true,
    score FLOAT8,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    birth_date DATE
);
ALTER TABLE e2e_column_types REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_column_types FOR TABLE e2e_column_types;
-- DATA --
INSERT INTO e2e_column_types (id, name, age, balance, is_active, score, created_at, birth_date) VALUES
    (1, 'alice', 30, 1000.50, true, 95.5, '2025-01-15 10:30:00+00', '1995-03-20'),
    (2, 'bob', 25, NULL, false, NULL, '2025-06-20 14:00:00+00', NULL),
    (3, 'charlie', 35, 5000.00, true, 88.2, '2025-12-01 08:15:30+00', '1990-07-04');
