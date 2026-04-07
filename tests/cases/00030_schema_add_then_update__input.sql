-- SETUP --
CREATE TABLE e2e_schema_add_upd (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
ALTER TABLE e2e_schema_add_upd REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_schema_add_upd (id, name) VALUES
    (1, 'alice'),
    (2, 'bob');
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_add_upd ADD COLUMN score INTEGER;
-- SLEEP 3 --
-- DATA --
UPDATE e2e_schema_add_upd SET score = 50 WHERE id = 1;
INSERT INTO e2e_schema_add_upd (id, name, score) VALUES (3, 'charlie', 100);
UPDATE e2e_schema_add_upd SET score = 75 WHERE id = 2;
