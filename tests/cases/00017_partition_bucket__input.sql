-- SETUP --
CREATE TABLE e2e_partition_bucket (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
ALTER TABLE e2e_partition_bucket REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_partition_bucket (id, name) VALUES
    (1, 'alice'),
    (2, 'bob'),
    (3, 'charlie'),
    (4, 'dave'),
    (5, 'eve');
