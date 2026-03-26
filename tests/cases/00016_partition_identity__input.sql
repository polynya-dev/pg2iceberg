-- SETUP --
CREATE TABLE e2e_partition_identity (
    id SERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    name TEXT NOT NULL
);
ALTER TABLE e2e_partition_identity REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_partition_identity FOR TABLE e2e_partition_identity;
-- DATA --
INSERT INTO e2e_partition_identity (id, region, name) VALUES
    (1, 'us-east', 'alice'),
    (2, 'us-east', 'bob'),
    (3, 'eu-west', 'charlie'),
    (4, 'eu-west', 'dave'),
    (5, 'ap-south', 'eve');
