-- SETUP --
CREATE TABLE e2e_toast_col_then_update (
    id SERIAL PRIMARY KEY,
    label TEXT NOT NULL,
    payload TEXT NOT NULL
);
ALTER TABLE e2e_toast_col_then_update ALTER COLUMN payload SET STORAGE EXTERNAL;
-- DATA --
INSERT INTO e2e_toast_col_then_update (id, label, payload) VALUES
    (1, 'orig', repeat('A', 100000));

-- First update: only label changes, payload is TOAST-unchanged.
-- The unchanged payload is resolved from the in-memory row cache,
-- preserving the correct historical value repeat('A', 100000).
UPDATE e2e_toast_col_then_update SET label = 'step1' WHERE id = 1;

-- Second update: payload itself changes. This is sent as 't' (full value).
UPDATE e2e_toast_col_then_update SET payload = repeat('Z', 100000) WHERE id = 1;
