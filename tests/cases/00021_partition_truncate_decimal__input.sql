-- SETUP --
CREATE TABLE e2e_partition_truncate_decimal (
    id SERIAL PRIMARY KEY,
    price NUMERIC(10,2) NOT NULL,
    name TEXT NOT NULL
);
ALTER TABLE e2e_partition_truncate_decimal REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_partition_truncate_decimal FOR TABLE e2e_partition_truncate_decimal;
-- DATA --
INSERT INTO e2e_partition_truncate_decimal (id, price, name) VALUES
    (1, 10.65, 'alice'),
    (2, 25.30, 'bob'),
    (3, 99.99, 'charlie'),
    (4, -10.65, 'dave'),
    (5, 0.00, 'eve');
