-- SETUP --
CREATE TABLE e2e_toast_update_delete (
    id SERIAL PRIMARY KEY,
    label TEXT NOT NULL,
    payload TEXT NOT NULL
);
ALTER TABLE e2e_toast_update_delete ALTER COLUMN payload SET STORAGE EXTERNAL;
-- DATA --
INSERT INTO e2e_toast_update_delete (id, label, payload) VALUES
    (1, 'keep', repeat('A', 100000)),
    (2, 'delete_me', repeat('B', 100000));

-- Update row 2 (TOAST unchanged), then delete it.
-- The unchanged payload is resolved from the in-memory row cache,
-- so the update row retains the correct payload value.
-- The subsequent DELETE's equality delete cancels it out anyway.
UPDATE e2e_toast_update_delete SET label = 'changed' WHERE id = 2;
DELETE FROM e2e_toast_update_delete WHERE id = 2;
