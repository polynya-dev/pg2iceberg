-- SETUP --
CREATE TABLE e2e_toast_part_xflush (
    id SERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    label TEXT NOT NULL,
    payload TEXT NOT NULL
);
ALTER TABLE e2e_toast_part_xflush ALTER COLUMN payload SET STORAGE EXTERNAL;
-- DATA --
INSERT INTO e2e_toast_part_xflush (id, region, label, payload) VALUES
    (1, 'us', 'orig', repeat('A', 100000)),
    (2, 'eu', 'orig', repeat('B', 100000));
-- SLEEP 5 --
-- DATA --
-- Update only label (non-TOAST). payload is unchanged, so PostgreSQL sends it
-- as 'u' (unchanged TOAST). The rowCache is empty because the INSERT was
-- flushed in the previous batch, so resolveToast must scan Iceberg data files.
-- With a partitioned table, the partition filter must correctly unwrap Avro
-- union values to match data files.
UPDATE e2e_toast_part_xflush SET label = 'updated' WHERE id = 1;
UPDATE e2e_toast_part_xflush SET label = 'updated' WHERE id = 2;
