-- SETUP --
CREATE TABLE e2e_delete (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
ALTER TABLE e2e_delete REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_delete (id, name) VALUES
    (1, 'alice'),
    (2, 'bob'),
    (3, 'charlie'),
    (4, 'diana');

-- NOTE: ClickHouse 26.2 does not apply Iceberg equality deletes yet,
-- so the reference output still includes the deleted rows.
DELETE FROM e2e_delete WHERE id IN (2, 4);
