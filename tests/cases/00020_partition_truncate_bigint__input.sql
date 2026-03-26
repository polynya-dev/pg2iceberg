-- SETUP --
CREATE TABLE e2e_partition_truncate_bigint (
    id SERIAL PRIMARY KEY,
    amount BIGINT NOT NULL,
    name TEXT NOT NULL
);
ALTER TABLE e2e_partition_truncate_bigint REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_partition_truncate_bigint FOR TABLE e2e_partition_truncate_bigint;
-- DATA --
INSERT INTO e2e_partition_truncate_bigint (id, amount, name) VALUES
    (1, 150, 'alice'),
    (2, 250, 'bob'),
    (3, -50, 'charlie'),
    (4, 999, 'dave'),
    (5, 0, 'eve');
