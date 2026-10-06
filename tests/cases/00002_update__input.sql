-- SETUP --
CREATE TABLE e2e_update (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    score INTEGER NOT NULL
);
ALTER TABLE e2e_update REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_update (id, name, score) VALUES
    (1, 'alice', 10),
    (2, 'bob', 20),
    (3, 'charlie', 30);

UPDATE e2e_update SET score = 99 WHERE id = 2;
