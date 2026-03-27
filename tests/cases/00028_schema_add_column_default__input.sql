-- SETUP --
CREATE TABLE e2e_schema_add_default (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
ALTER TABLE e2e_schema_add_default REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_schema_add_default FOR TABLE e2e_schema_add_default;
-- DATA --
INSERT INTO e2e_schema_add_default (id, name) VALUES
    (1, 'alice'),
    (2, 'bob');
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_add_default ADD COLUMN status TEXT DEFAULT 'active';
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_add_default (id, name) VALUES
    (3, 'charlie');
INSERT INTO e2e_schema_add_default (id, name, status) VALUES
    (4, 'dave', 'inactive');
