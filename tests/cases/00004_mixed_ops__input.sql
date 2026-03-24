-- SETUP --
CREATE TABLE e2e_mixed_ops (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
);
ALTER TABLE e2e_mixed_ops REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_mixed_ops FOR TABLE e2e_mixed_ops;
-- DATA --
INSERT INTO e2e_mixed_ops (id, name, status) VALUES
    (1, 'alice', 'active'),
    (2, 'bob', 'active'),
    (3, 'charlie', 'active'),
    (4, 'diana', 'active'),
    (5, 'eve', 'active');

-- NOTE: ClickHouse 26.2 does not apply Iceberg equality deletes yet,
-- so the reference output includes both old and new rows for updates/deletes.
UPDATE e2e_mixed_ops SET status = 'inactive' WHERE id IN (1, 3);
DELETE FROM e2e_mixed_ops WHERE id = 5;
INSERT INTO e2e_mixed_ops (id, name, status) VALUES (6, 'frank', 'active');
UPDATE e2e_mixed_ops SET name = 'robert' WHERE id = 2;
