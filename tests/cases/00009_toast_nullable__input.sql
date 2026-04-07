-- SETUP --
CREATE TABLE e2e_toast_nullable (
    id SERIAL PRIMARY KEY,
    label TEXT NOT NULL,
    bio TEXT,
    payload TEXT
);
ALTER TABLE e2e_toast_nullable ALTER COLUMN bio SET STORAGE EXTERNAL;
ALTER TABLE e2e_toast_nullable ALTER COLUMN payload SET STORAGE EXTERNAL;
-- DATA --
INSERT INTO e2e_toast_nullable (id, label, bio, payload) VALUES
    (1, 'with_both', repeat('A', 100000), repeat('B', 100000)),
    (2, 'bio_only', repeat('C', 100000), NULL),
    (3, 'payload_only', NULL, repeat('D', 100000));

-- Update non-TOAST column on all rows.
-- Row 1: both TOAST cols unchanged (both have values)
-- Row 2: bio unchanged (has value), payload unchanged (NULL — sent as 'n' not 'u')
-- Row 3: bio unchanged (NULL — sent as 'n' not 'u'), payload unchanged (has value)
UPDATE e2e_toast_nullable SET label = 'updated' WHERE id IN (1, 2, 3);
