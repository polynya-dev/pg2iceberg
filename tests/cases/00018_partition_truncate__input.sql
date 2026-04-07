-- SETUP --
CREATE TABLE e2e_partition_truncate (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
ALTER TABLE e2e_partition_truncate REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_partition_truncate (id, name) VALUES
    (1, 'alice'),
    (2, 'bob'),
    (3, 'charlie'),
    (4, 'dave'),
    (5, 'eve');
