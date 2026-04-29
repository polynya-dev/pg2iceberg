-- Seed data for Rideshare App

INSERT INTO riders (email, first_name, last_name, phone, city, signed_up_at, last_ride_at) VALUES
  ('hasyimi+maria@hasyimibahrudin.com',    'Maria',   'Santos',     '+1-555-0101', 'San Francisco', '2025-06-01 10:00:00+00', '2026-03-20 22:00:00+00'),
  ('hasyimi+james@hasyimibahrudin.com',    'James',   'Lee',        '+1-555-0102', 'San Francisco', '2025-08-15 14:00:00+00', '2026-03-19 08:30:00+00'),
  ('hasyimi+aisha@hasyimibahrudin.com',    'Aisha',   'Patel',      '+1-555-0103', 'New York',      '2025-10-20 09:00:00+00', '2026-03-21 07:00:00+00'),
  ('hasyimi+tom@hasyimibahrudin.com',      'Tom',     'Eriksson',   '+1-555-0104', 'New York',      '2026-01-05 16:00:00+00', '2026-02-14 18:00:00+00'),
  ('hasyimi+lin@hasyimibahrudin.com',      'Lin',     'Wei',        '+1-555-0105', 'San Francisco', '2026-01-20 11:00:00+00', '2026-03-20 19:00:00+00'),
  ('hasyimi+priya@hasyimibahrudin.com',    'Priya',   'Sharma',     '+1-555-0106', 'Austin',        '2026-02-01 13:00:00+00', '2026-03-18 17:00:00+00'),
  ('hasyimi+omar@hasyimibahrudin.com',     'Omar',    'Hassan',     '+1-555-0107', 'Austin',        '2026-02-15 08:00:00+00', NULL),
  ('hasyimi+sophie@hasyimibahrudin.com',   'Sophie',  'Müller',     '+1-555-0108', 'New York',      '2026-03-01 10:00:00+00', '2026-03-15 12:00:00+00'),
  ('hasyimi+carlos@hasyimibahrudin.com',   'Carlos',  'Reyes',      '+1-555-0109', 'San Francisco', '2026-03-10 07:00:00+00', '2026-03-10 09:00:00+00'),
  ('hasyimi+nina@hasyimibahrudin.com',     'Nina',    'Kowalski',   '+1-555-0110', 'Austin',        '2026-03-18 15:00:00+00', NULL);

INSERT INTO drivers (email, first_name, last_name, phone, city, vehicle_make, vehicle_model, vehicle_year, license_plate, rating, status, signed_up_at) VALUES
  ('hasyimi+driver.ali@hasyimibahrudin.com',    'Ali',      'Reza',     '+1-555-0201', 'San Francisco', 'Toyota',  'Camry',   2023, '7ABC123', 4.92, 'active',    '2025-05-01 08:00:00+00'),
  ('hasyimi+driver.chen@hasyimibahrudin.com',   'Wei',      'Chen',     '+1-555-0202', 'San Francisco', 'Honda',   'Civic',   2024, '8DEF456', 4.85, 'active',    '2025-07-10 12:00:00+00'),
  ('hasyimi+driver.jones@hasyimibahrudin.com',  'Marcus',   'Jones',    '+1-555-0203', 'New York',      'Hyundai', 'Ioniq 5', 2025, '3GHI789', 4.97, 'active',    '2025-09-01 06:00:00+00'),
  ('hasyimi+driver.beck@hasyimibahrudin.com',   'Anna',     'Beck',     '+1-555-0204', 'New York',      'Tesla',   'Model 3', 2024, '1JKL012', 4.78, 'active',    '2025-11-15 10:00:00+00'),
  ('hasyimi+driver.diaz@hasyimibahrudin.com',   'Luis',     'Diaz',     '+1-555-0205', 'Austin',        'Ford',    'Mach-E',  2025, '5MNO345', 4.90, 'active',    '2026-01-10 09:00:00+00'),
  ('hasyimi+driver.kim@hasyimibahrudin.com',    'Soo-jin',  'Kim',      '+1-555-0206', 'Austin',        'Kia',     'EV6',     2024, '2PQR678', 3.95, 'suspended', '2026-01-20 14:00:00+00');

-- Completed rides
INSERT INTO rides (rider_id, driver_id, status, pickup_address, dropoff_address, distance_km, fare_cents, requested_at, picked_up_at, dropped_off_at) VALUES
  (1, 1, 'completed', '123 Market St, SF',         'SFO Airport',                  21.5,  4250, '2026-03-20 18:00:00+00', '2026-03-20 18:12:00+00', '2026-03-20 18:45:00+00'),
  (1, 2, 'completed', 'SFO Airport',               '123 Market St, SF',            21.5,  3900, '2026-03-20 21:30:00+00', '2026-03-20 21:40:00+00', '2026-03-20 22:00:00+00'),
  (2, 1, 'completed', '456 Mission St, SF',        '789 Valencia St, SF',           3.2,  1200, '2026-03-19 08:00:00+00', '2026-03-19 08:05:00+00', '2026-03-19 08:18:00+00'),
  (3, 3, 'completed', '10 Columbus Circle, NY',    'JFK Airport',                  28.0,  5500, '2026-03-21 05:00:00+00', '2026-03-21 05:10:00+00', '2026-03-21 06:00:00+00'),
  (3, 4, 'completed', '350 5th Ave, NY',           '200 Broadway, NY',              2.1,   950, '2026-03-18 12:00:00+00', '2026-03-18 12:04:00+00', '2026-03-18 12:15:00+00'),
  (5, 2, 'completed', '100 Embarcadero, SF',       '500 Castro St, SF',             5.8,  1800, '2026-03-20 18:30:00+00', '2026-03-20 18:38:00+00', '2026-03-20 19:00:00+00'),
  (6, 5, 'completed', '1100 Congress Ave, Austin',  '4700 E Riverside, Austin',     8.5,  2200, '2026-03-18 16:30:00+00', '2026-03-18 16:38:00+00', '2026-03-18 17:00:00+00'),
  (8, 3, 'completed', '1 Penn Plaza, NY',           '88 Greenwich St, NY',          3.0,  1100, '2026-03-15 11:30:00+00', '2026-03-15 11:36:00+00', '2026-03-15 11:50:00+00'),
  (9, 1, 'completed', '1 Ferry Building, SF',       '2000 Folsom St, SF',           4.5,  1500, '2026-03-10 08:30:00+00', '2026-03-10 08:36:00+00', '2026-03-10 09:00:00+00');

-- In-progress ride
INSERT INTO rides (rider_id, driver_id, status, pickup_address, dropoff_address, distance_km, fare_cents, requested_at, picked_up_at) VALUES
  (3, 4, 'in_progress', 'JFK Airport', '350 5th Ave, NY', 28.0, NULL, '2026-03-21 06:30:00+00', '2026-03-21 06:45:00+00');

-- Cancelled ride
INSERT INTO rides (rider_id, driver_id, status, pickup_address, dropoff_address, requested_at) VALUES
  (4, 4, 'cancelled', '100 W 72nd St, NY', 'LaGuardia Airport', '2026-02-14 17:00:00+00');

-- Payments for completed rides
INSERT INTO payments (ride_id, rider_id, amount_cents, method, status, charged_at) VALUES
  (1,  1, 4250, 'card',   'charged', '2026-03-20 18:46:00+00'),
  (2,  1, 3900, 'card',   'charged', '2026-03-20 22:01:00+00'),
  (3,  2, 1200, 'wallet', 'charged', '2026-03-19 08:19:00+00'),
  (4,  3, 5500, 'card',   'charged', '2026-03-21 06:01:00+00'),
  (5,  3,  950, 'card',   'charged', '2026-03-18 12:16:00+00'),
  (6,  5, 1800, 'wallet', 'charged', '2026-03-20 19:01:00+00'),
  (7,  6, 2200, 'card',   'charged', '2026-03-18 17:01:00+00'),
  (8,  8, 1100, 'card',   'charged', '2026-03-15 11:51:00+00'),
  (9,  9, 1500, 'card',   'charged', '2026-03-10 09:01:00+00');

-- Ratings
INSERT INTO ratings (ride_id, from_rider, score, comment, created_at) VALUES
  (1, true,  5, 'Smooth ride, great driver!',   '2026-03-20 18:50:00+00'),
  (1, false, 5, NULL,                            '2026-03-20 18:55:00+00'),
  (2, true,  4, NULL,                            '2026-03-20 22:05:00+00'),
  (3, true,  5, 'Quick pickup',                  '2026-03-19 08:20:00+00'),
  (4, true,  5, 'Very professional',             '2026-03-21 06:05:00+00'),
  (5, true,  4, NULL,                            '2026-03-18 12:20:00+00'),
  (7, true,  5, 'Love the EV!',                  '2026-03-18 17:05:00+00'),
  (8, true,  5, NULL,                            '2026-03-15 11:55:00+00'),
  (9, true,  4, 'Good but took a longer route',  '2026-03-10 09:05:00+00');
