-- Smoke-test extras, after schema + seed.
-- drivers: Postgres's default replica identity (a DELETE/UPDATE's old
-- tuple carries only the key) and a TOAST-able column.
ALTER TABLE drivers REPLICA IDENTITY DEFAULT;
ALTER TABLE drivers ADD COLUMN notes text;
-- More drivers and riders so updates spread out.
INSERT INTO drivers (email, first_name, last_name, phone, city, vehicle_make, vehicle_model, vehicle_year, license_plate, rating, status, signed_up_at)
SELECT 'smoke.driver.' || g || '@example.com', 'D' || g, 'Smoke', '+1-555-9' || g, (ARRAY['San Francisco','New York','Austin'])[1 + g % 3],
       'Toyota', 'Prius', 2024, 'SMK' || g, 4.5, 'active', now()
FROM generate_series(1, 40) g;
INSERT INTO riders (email, first_name, last_name, phone, city, signed_up_at)
SELECT 'smoke.rider.' || g || '@example.com', 'R' || g, 'Smoke', '+1-555-8' || g, (ARRAY['San Francisco','New York','Austin'])[1 + g % 3], now()
FROM generate_series(1, 200) g;
