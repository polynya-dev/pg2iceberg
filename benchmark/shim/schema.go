package main

// Rideshare schema — mirrors example/single/schema.sql, with indexes on
// the columns the traffic handlers filter on so Postgres doesn't become
// the bottleneck, and REPLICA IDENTITY FULL so logical replication
// carries full old-row images (heavier WAL = more realistic CDC load).
const schemaDDL = `
CREATE TABLE IF NOT EXISTS riders (
  id            SERIAL PRIMARY KEY,
  email         TEXT NOT NULL UNIQUE,
  first_name    TEXT NOT NULL,
  last_name     TEXT,
  phone         TEXT,
  city          TEXT NOT NULL,
  signed_up_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_ride_at  TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS drivers (
  id              SERIAL PRIMARY KEY,
  email           TEXT NOT NULL UNIQUE,
  first_name      TEXT NOT NULL,
  last_name       TEXT,
  phone           TEXT NOT NULL,
  city            TEXT NOT NULL,
  vehicle_make    TEXT NOT NULL,
  vehicle_model   TEXT NOT NULL,
  vehicle_year    INT NOT NULL,
  license_plate   TEXT NOT NULL,
  rating          NUMERIC(3,2) NOT NULL DEFAULT 5.00,
  status          TEXT NOT NULL DEFAULT 'active',
  signed_up_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS rides (
  id              SERIAL PRIMARY KEY,
  rider_id        INT NOT NULL REFERENCES riders(id),
  driver_id       INT REFERENCES drivers(id),
  status          TEXT NOT NULL DEFAULT 'requested',
  pickup_address  TEXT NOT NULL,
  dropoff_address TEXT NOT NULL,
  distance_km     NUMERIC(6,2),
  fare_cents      INT,
  requested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  picked_up_at    TIMESTAMPTZ,
  dropped_off_at  TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS payments (
  id            SERIAL PRIMARY KEY,
  ride_id       INT NOT NULL REFERENCES rides(id),
  rider_id      INT NOT NULL REFERENCES riders(id),
  amount_cents  INT NOT NULL,
  method        TEXT NOT NULL DEFAULT 'card',
  status        TEXT NOT NULL DEFAULT 'pending',
  charged_at    TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS ratings (
  id          SERIAL PRIMARY KEY,
  ride_id     INT NOT NULL REFERENCES rides(id),
  from_rider  BOOLEAN NOT NULL,
  score       INT NOT NULL CHECK (score BETWEEN 1 AND 5),
  comment     TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rides_status ON rides(status);
CREATE INDEX IF NOT EXISTS idx_drivers_city_status ON drivers(city, status);

ALTER TABLE riders   REPLICA IDENTITY FULL;
ALTER TABLE drivers  REPLICA IDENTITY FULL;
ALTER TABLE rides    REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;
ALTER TABLE ratings  REPLICA IDENTITY FULL;
`

// Generative seed via generate_series so the initial snapshot has real
// volume to copy. Each statement is executed standalone with its own
// args, so parameters are numbered per-statement ($1 first), not shared
// across statements.
const seedRiders = `
INSERT INTO riders (email, first_name, last_name, phone, city, signed_up_at)
SELECT
  'seed.rider.'||g||'@bench.local',
  (ARRAY['Liam','Olivia','Noah','Ava','Ethan','Sophia','Mason','Mia','Lucas','Harper'])[1+floor(random()*10)::int],
  (ARRAY['Smith','Park','Garcia','Nakamura','Brown','Singh','Costa','Kim','Chen','Taylor'])[1+floor(random()*10)::int],
  '+1-555-'||lpad((floor(random()*10000))::text,4,'0'),
  (ARRAY['San Francisco','New York','Austin','Los Angeles','Chicago','Seattle'])[1+floor(random()*6)::int],
  now() - (floor(random()*365))::int * interval '1 day'
FROM generate_series(1, $1::int) g;
`

const seedDrivers = `
INSERT INTO drivers (email, first_name, last_name, phone, city, vehicle_make, vehicle_model, vehicle_year, license_plate, status, signed_up_at)
SELECT
  'seed.driver.'||g||'@bench.local',
  (ARRAY['Liam','Olivia','Noah','Ava','Ethan','Sophia','Mason','Mia','Lucas','Harper'])[1+floor(random()*10)::int],
  (ARRAY['Smith','Park','Garcia','Nakamura','Brown','Singh','Costa','Kim','Chen','Taylor'])[1+floor(random()*10)::int],
  '+1-555-'||lpad((floor(random()*10000))::text,4,'0'),
  (ARRAY['San Francisco','New York','Austin','Los Angeles','Chicago','Seattle'])[1+floor(random()*6)::int],
  (ARRAY['Toyota','Honda','Tesla','Hyundai','Ford','Kia'])[1+floor(random()*6)::int],
  (ARRAY['Camry','Civic','Model 3','Ioniq 5','Mach-E','EV6'])[1+floor(random()*6)::int],
  2023 + floor(random()*3)::int,
  'P'||lpad(g::text,6,'0'),
  CASE WHEN random() < 0.95 THEN 'active' ELSE 'suspended' END,
  now() - (floor(random()*400))::int * interval '1 day'
FROM generate_series(1, $1::int) g;
`

const seedRides = `
INSERT INTO rides (rider_id, driver_id, status, pickup_address, dropoff_address, distance_km, fare_cents, requested_at, picked_up_at, dropped_off_at)
SELECT
  1 + floor(random()*$1::int)::int,
  1 + floor(random()*$2::int)::int,
  'completed',
  'addr-'||floor(random()*1000)::int,
  'addr-'||floor(random()*1000)::int,
  round((1.5 + random()*28.5)::numeric, 2),
  (500 + random()*5000)::int,
  now() - (floor(random()*30))::int * interval '1 day',
  now() - (floor(random()*30))::int * interval '1 day',
  now() - (floor(random()*30))::int * interval '1 day'
FROM generate_series(1, $3::int) g;
`

const seedPayments = `
INSERT INTO payments (ride_id, rider_id, amount_cents, method, status, charged_at)
SELECT id, rider_id, fare_cents, (ARRAY['card','card','wallet'])[1+floor(random()*3)::int], 'charged', dropped_off_at
FROM rides WHERE status='completed' AND fare_cents IS NOT NULL;
`

const seedRatings = `
INSERT INTO ratings (ride_id, from_rider, score, comment, created_at)
SELECT id, true, (ARRAY[5,5,5,4,4,3,2,1])[1+floor(random()*8)::int], NULL, dropped_off_at
FROM rides WHERE status='completed' AND random() < 0.8;
`
