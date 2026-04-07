-- SETUP --
CREATE TABLE e2e_schema_add_column (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
ALTER TABLE e2e_schema_add_column REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_schema_add_column (id, name) VALUES
    (1, 'alice'),
    (2, 'bob');
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_add_column ADD COLUMN score INTEGER;
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_add_column (id, name, score) VALUES
    (3, 'charlie', 100),
    (4, 'dave', 200);
