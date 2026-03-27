-- SETUP --
CREATE TABLE e2e_schema_drop_col (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    temp TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);
ALTER TABLE e2e_schema_drop_col REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_schema_drop_col FOR TABLE e2e_schema_drop_col;
-- DATA --
INSERT INTO e2e_schema_drop_col (id, name, temp, amount) VALUES
    (1, 'alice', 'x', 100.50),
    (2, 'bob', 'y', 200.75);
-- SLEEP 5 --
-- DATA --
ALTER TABLE e2e_schema_drop_col DROP COLUMN temp;
-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_schema_drop_col (id, name, amount) VALUES
    (3, 'charlie', 300.00),
    (4, 'dave', 400.00);
