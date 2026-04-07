-- SETUP --
CREATE TABLE e2e_update (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    score INTEGER NOT NULL
);
ALTER TABLE e2e_update REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_update (id, name, score) VALUES
    (1, 'alice', 10),
    (2, 'bob', 20),
    (3, 'charlie', 30);

-- NOTE: ClickHouse 26.2 does not apply Iceberg equality deletes yet,
-- so the reference output includes both the old and new rows for updated records.
UPDATE e2e_update SET score = 99 WHERE id = 2;
