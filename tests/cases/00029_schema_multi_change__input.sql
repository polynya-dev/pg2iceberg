-- SETUP --
CREATE TABLE e2e_schema_multi (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    temp TEXT,
    value INTEGER
);
ALTER TABLE e2e_schema_multi REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_schema_multi FOR TABLE e2e_schema_multi;
-- DATA --
INSERT INTO e2e_schema_multi (id, name, temp, value) VALUES
    (1, 'alice', 'x', 100),
    (2, 'bob', 'y', 200);
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_multi DROP COLUMN temp, ADD COLUMN score BIGINT, ALTER COLUMN value TYPE BIGINT;
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_multi (id, name, value, score) VALUES
    (3, 'charlie', 3000000000, 90),
    (4, 'dave', 4000000000, 95);
