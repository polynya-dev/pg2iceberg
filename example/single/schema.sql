-- Rideshare App — a hypothetical ride-sharing platform
-- Tables: riders, drivers, rides, payments, ratings

CREATE TABLE riders (
  id            SERIAL PRIMARY KEY,
  email         TEXT NOT NULL UNIQUE,
  first_name    TEXT NOT NULL,
  last_name     TEXT,
  phone         TEXT,
  city          TEXT NOT NULL,
  signed_up_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_ride_at  TIMESTAMPTZ
);

CREATE TABLE drivers (
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
  status          TEXT NOT NULL DEFAULT 'active',      -- active | suspended | inactive
  signed_up_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE rides (
  id              SERIAL PRIMARY KEY,
  rider_id        INT NOT NULL REFERENCES riders(id),
  driver_id       INT REFERENCES drivers(id),
  status          TEXT NOT NULL DEFAULT 'requested',   -- requested | matched | in_progress | completed | cancelled
  pickup_address  TEXT NOT NULL,
  dropoff_address TEXT NOT NULL,
  distance_km     NUMERIC(6,2),
  fare_cents      INT,
  requested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  picked_up_at    TIMESTAMPTZ,
  dropped_off_at  TIMESTAMPTZ
);

CREATE TABLE payments (
  id            SERIAL PRIMARY KEY,
  ride_id       INT NOT NULL REFERENCES rides(id),
  rider_id      INT NOT NULL REFERENCES riders(id),
  amount_cents  INT NOT NULL,
  method        TEXT NOT NULL DEFAULT 'card',           -- card | wallet | cash
  status        TEXT NOT NULL DEFAULT 'pending',        -- pending | charged | refunded
  charged_at    TIMESTAMPTZ
);

CREATE TABLE ratings (
  id          SERIAL PRIMARY KEY,
  ride_id     INT NOT NULL REFERENCES rides(id),
  from_rider  BOOLEAN NOT NULL,                         -- true = rider rated driver, false = driver rated rider
  score       INT NOT NULL CHECK (score BETWEEN 1 AND 5),
  comment     TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Logical replication setup for pg2iceberg
ALTER TABLE riders   REPLICA IDENTITY FULL;
ALTER TABLE drivers  REPLICA IDENTITY FULL;
ALTER TABLE rides    REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;
ALTER TABLE ratings  REPLICA IDENTITY FULL;
