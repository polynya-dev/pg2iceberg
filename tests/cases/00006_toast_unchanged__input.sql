-- SETUP --
CREATE TABLE e2e_toast_unchanged (
    id SERIAL PRIMARY KEY,
    label TEXT NOT NULL,
    payload TEXT NOT NULL
);
-- Force out-of-line storage so TOAST always kicks in.
ALTER TABLE e2e_toast_unchanged ALTER COLUMN payload SET STORAGE EXTERNAL;
-- NOTE: deliberately NOT using REPLICA IDENTITY FULL.
-- Default replica identity (using PK) means unchanged TOAST columns
-- are sent as 'u' (unchanged) in the WAL stream.
-- DATA --
INSERT INTO e2e_toast_unchanged (id, label, payload) VALUES
    (1, 'row1', repeat('A', 100000)),
    (2, 'row2', repeat('B', 100000)),
    (3, 'row3', repeat('C', 100000));

-- Update only the non-TOAST column. The payload column is TOASTed and
-- unchanged, so PostgreSQL sends it as 'u'. pg2iceberg must preserve
-- the original payload value.
UPDATE e2e_toast_unchanged SET label = 'row2_updated' WHERE id = 2;
