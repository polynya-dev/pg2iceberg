-- SETUP --
CREATE TABLE e2e_schema_widen_float (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    score REAL NOT NULL
);
ALTER TABLE e2e_schema_widen_float REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_schema_widen_float FOR TABLE e2e_schema_widen_float;
-- DATA --
INSERT INTO e2e_schema_widen_float (id, name, score) VALUES
    (1, 'alice', 1.5),
    (2, 'bob', 2.5);
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_widen_float ALTER COLUMN score TYPE DOUBLE PRECISION;
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_widen_float (id, name, score) VALUES
    (3, 'charlie', 3.141592653589793),
    (4, 'dave', 2.718281828459045);
