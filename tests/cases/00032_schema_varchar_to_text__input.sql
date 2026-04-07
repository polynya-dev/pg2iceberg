-- SETUP --
CREATE TABLE e2e_schema_varchar (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    value INTEGER
);
ALTER TABLE e2e_schema_varchar REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_schema_varchar (id, name, value) VALUES
    (1, 'alice', 100),
    (2, 'bob', 200);
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_varchar ALTER COLUMN name TYPE TEXT;
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_varchar (id, name, value) VALUES
    (3, 'charlie_with_a_very_long_name_exceeding_fifty_characters_easily', 300),
    (4, 'dave', 400);
