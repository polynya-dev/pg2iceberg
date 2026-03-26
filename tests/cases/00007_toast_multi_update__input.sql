-- SETUP --
CREATE TABLE e2e_toast_multi_update (
    id SERIAL PRIMARY KEY,
    label TEXT NOT NULL,
    payload TEXT NOT NULL
);
ALTER TABLE e2e_toast_multi_update ALTER COLUMN payload SET STORAGE EXTERNAL;
CREATE PUBLICATION pg2iceberg_pub_e2e_toast_multi_update FOR TABLE e2e_toast_multi_update;
-- DATA --
INSERT INTO e2e_toast_multi_update (id, label, payload) VALUES
    (1, 'orig', repeat('X', 100000));

-- Two updates to the same row — only label changes, payload is TOAST-unchanged both times.
UPDATE e2e_toast_multi_update SET label = 'first' WHERE id = 1;
UPDATE e2e_toast_multi_update SET label = 'second' WHERE id = 1;
