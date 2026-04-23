-- Ten seed accounts so the simulator has something to move money between.
INSERT INTO accounts (email, balance) VALUES
    ('alice@example.com',   1000),
    ('bob@example.com',      800),
    ('carol@example.com',    600),
    ('dan@example.com',     1200),
    ('erin@example.com',     400),
    ('frank@example.com',    900),
    ('gina@example.com',     700),
    ('henry@example.com',    500),
    ('ivy@example.com',     1100),
    ('julia@example.com',    300)
ON CONFLICT (email) DO NOTHING;
