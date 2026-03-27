-- SETUP --
CREATE TABLE e2e_txn_insert (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);
ALTER TABLE e2e_txn_insert REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_txn_insert FOR TABLE e2e_txn_insert;
-- DATA --
BEGIN;
INSERT INTO e2e_txn_insert (id, name, amount) VALUES (1, 'alice', 100.00);
INSERT INTO e2e_txn_insert (id, name, amount) VALUES (2, 'bob', 200.00);
INSERT INTO e2e_txn_insert (id, name, amount) VALUES (3, 'charlie', 300.00);
COMMIT;
BEGIN;
INSERT INTO e2e_txn_insert (id, name, amount) VALUES (4, 'dave', 400.00);
INSERT INTO e2e_txn_insert (id, name, amount) VALUES (5, 'eve', 500.00);
COMMIT;
