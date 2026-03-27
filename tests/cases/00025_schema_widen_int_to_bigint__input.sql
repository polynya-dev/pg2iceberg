-- SETUP --
CREATE TABLE e2e_schema_widen_int (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value INTEGER NOT NULL
);
ALTER TABLE e2e_schema_widen_int REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_schema_widen_int FOR TABLE e2e_schema_widen_int;
-- DATA --
INSERT INTO e2e_schema_widen_int (id, name, value) VALUES
    (1, 'alice', 100),
    (2, 'bob', 200);
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_widen_int ALTER COLUMN value TYPE BIGINT;
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_widen_int (id, name, value) VALUES
    (3, 'charlie', 3000000000),
    (4, 'dave', 4000000000);
