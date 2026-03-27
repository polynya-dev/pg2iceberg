-- SETUP --
CREATE TABLE e2e_schema_seq (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
ALTER TABLE e2e_schema_seq REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_schema_seq FOR TABLE e2e_schema_seq;
-- DATA --
INSERT INTO e2e_schema_seq (id, name) VALUES (1, 'alice');
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_seq ADD COLUMN score INTEGER;
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_seq (id, name, score) VALUES (2, 'bob', 100);
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_seq ADD COLUMN level TEXT;
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_seq (id, name, score, level) VALUES (3, 'charlie', 200, 'gold');
