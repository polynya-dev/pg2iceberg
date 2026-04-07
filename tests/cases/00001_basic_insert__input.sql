-- SETUP --
CREATE TABLE e2e_basic_insert (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);
ALTER TABLE e2e_basic_insert REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_basic_insert (id, name, amount) VALUES
    (1, 'alice', 100.50),
    (2, 'bob', 200.75),
    (3, 'charlie', 300.00);
