-- SETUP --
CREATE TABLE e2e_schema_rename (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value INTEGER
);
ALTER TABLE e2e_schema_rename REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_schema_rename FOR TABLE e2e_schema_rename;
-- DATA --
INSERT INTO e2e_schema_rename (id, name, value) VALUES
    (1, 'alice', 100),
    (2, 'bob', 200);
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_rename RENAME COLUMN value TO score;
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_rename (id, name, score) VALUES
    (3, 'charlie', 300),
    (4, 'dave', 400);
