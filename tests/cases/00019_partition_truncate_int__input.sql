-- SETUP --
CREATE TABLE e2e_partition_truncate_int (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
ALTER TABLE e2e_partition_truncate_int REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_partition_truncate_int FOR TABLE e2e_partition_truncate_int;
-- DATA --
INSERT INTO e2e_partition_truncate_int (id, name) VALUES
    (1, 'alice'),
    (15, 'bob'),
    (-1, 'charlie'),
    (25, 'dave'),
    (100, 'eve');
