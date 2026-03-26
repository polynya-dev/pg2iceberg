-- SETUP --
CREATE TABLE e2e_partition_truncate_binary (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    data BYTEA NOT NULL
);
ALTER TABLE e2e_partition_truncate_binary REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_partition_truncate_binary FOR TABLE e2e_partition_truncate_binary;
-- DATA --
INSERT INTO e2e_partition_truncate_binary (id, name, data) VALUES
    (1, 'alice',   '\x0102030405'),
    (2, 'bob',     '\xaabbccddee'),
    (3, 'charlie', '\xff00ff00ff'),
    (4, 'dave',    '\xdeadbeef'),
    (5, 'eve',     '\x0102');
