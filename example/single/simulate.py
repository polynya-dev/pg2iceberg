"""
Simulates realistic activity for a Rideshare app connected to GrowthPipe.
Generates new riders/drivers, ride requests, completions, payments, and ratings.

Usage:
  DATABASE_URL=postgres://... python simulate.py [--interval 5] [--once]
"""

import os
import sys
import time
import random
import argparse
from datetime import datetime, timezone, timedelta

import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://localhost:5432/rideshare")

FIRST_NAMES = ["Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason", "Mia", "Lucas", "Harper",
               "Aiden", "Ella", "Caden", "Aria", "Logan", "Riley", "Zara", "Leo", "Nora", "Kai"]
LAST_NAMES = ["Smith", "Park", "Garcia", "Nakamura", "Brown", "Singh", "Müller", "Costa", "Ali", "O'Brien",
              "Kowalski", "Fernandez", "Sato", "Williams", "Kim", "Chen", "Johansson", "Ahmed", "Taylor", "Rossi"]
CITIES = ["San Francisco", "New York", "Austin", "Los Angeles", "Chicago", "Seattle"]
VEHICLES = [
    ("Toyota", "Camry", 2023), ("Honda", "Civic", 2024), ("Tesla", "Model 3", 2025),
    ("Hyundai", "Ioniq 5", 2025), ("Ford", "Mach-E", 2024), ("Kia", "EV6", 2024),
    ("BMW", "i4", 2025), ("Chevrolet", "Bolt", 2024), ("Nissan", "Leaf", 2023),
]
SF_ADDRESSES = ["123 Market St", "456 Mission St", "789 Valencia St", "100 Embarcadero", "SFO Airport",
                "500 Castro St", "1 Ferry Building", "2000 Folsom St", "300 Brannan St", "Golden Gate Park"]
NY_ADDRESSES = ["350 5th Ave", "1 Penn Plaza", "200 Broadway", "JFK Airport", "88 Greenwich St",
                "10 Columbus Circle", "100 W 72nd St", "LaGuardia Airport", "42nd St Station", "Central Park"]
AUSTIN_ADDRESSES = ["1100 Congress Ave", "4700 E Riverside", "500 Barton Springs Rd", "Austin-Bergstrom Airport",
                    "2nd St District", "The Domain", "Zilker Park", "South Lamar Blvd"]
ADDRESSES = {"San Francisco": SF_ADDRESSES, "New York": NY_ADDRESSES, "Austin": AUSTIN_ADDRESSES,
             "Los Angeles": SF_ADDRESSES, "Chicago": NY_ADDRESSES, "Seattle": SF_ADDRESSES}

PAYMENT_METHODS = ["card", "card", "card", "wallet", "wallet"]
COMMENTS = [None, None, "Great ride!", "Smooth driving", "Quick pickup", "Very professional",
            "Love the EV!", "Took a longer route", "Friendly driver", "Clean car"]


def connect():
    return psycopg2.connect(DATABASE_URL)


def now():
    return datetime.now(timezone.utc)


def simulate_new_rider(conn):
    """New rider signs up."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email = f"hasyimi+{first.lower()}.{last.lower()}.{random.randint(100,999)}@hasyimibahrudin.com"
    city = random.choice(CITIES)

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO riders (email, first_name, last_name, phone, city, signed_up_at) VALUES (%s, %s, %s, %s, %s, %s)",
            (email, first, last, f"+1-555-{random.randint(1000,9999)}", city, now()),
        )
    conn.commit()
    print(f"[rider signup] {first} {last} in {city}")


def simulate_new_driver(conn):
    """New driver signs up."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    city = random.choice(CITIES)
    make, model, year = random.choice(VEHICLES)
    plate = f"{random.randint(1,9)}{chr(random.randint(65,90))}{chr(random.randint(65,90))}{chr(random.randint(65,90))}{random.randint(100,999)}"

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO drivers (email, first_name, last_name, phone, city, vehicle_make, vehicle_model, vehicle_year, license_plate, signed_up_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (f"hasyimi+driver.{first.lower()}{random.randint(10,99)}@hasyimibahrudin.com",
             first, last, f"+1-555-{random.randint(1000,9999)}",
             city, make, model, year, plate, now()),
        )
    conn.commit()
    print(f"[driver signup] {first} {last} — {make} {model} in {city}")


def simulate_request_ride(conn):
    """Rider requests a ride, gets matched to a driver."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, first_name, city FROM riders ORDER BY random() LIMIT 1")
        rider = cur.fetchone()
        if not rider:
            return
        rider_id, rider_name, city = rider

        cur.execute("SELECT id, first_name FROM drivers WHERE city = %s AND status = 'active' ORDER BY random() LIMIT 1", (city,))
        driver = cur.fetchone()

        addrs = ADDRESSES.get(city, SF_ADDRESSES)
        pickup = random.choice(addrs)
        dropoff = random.choice([a for a in addrs if a != pickup] or addrs)

        if driver:
            driver_id, driver_name = driver
            cur.execute(
                "INSERT INTO rides (rider_id, driver_id, status, pickup_address, dropoff_address, requested_at, picked_up_at) "
                "VALUES (%s, %s, 'in_progress', %s, %s, %s, %s) RETURNING id",
                (rider_id, driver_id, pickup, dropoff, now(), now() + timedelta(minutes=random.randint(3, 12))),
            )
            ride_id = cur.fetchone()[0]
            cur.execute("UPDATE riders SET last_ride_at = %s WHERE id = %s", (now(), rider_id))
            conn.commit()
            print(f"[ride] {rider_name} matched with {driver_name}: {pickup} → {dropoff}")
        else:
            cur.execute(
                "INSERT INTO rides (rider_id, status, pickup_address, dropoff_address, requested_at) "
                "VALUES (%s, 'requested', %s, %s, %s)",
                (rider_id, pickup, dropoff, now()),
            )
            conn.commit()
            print(f"[ride] {rider_name} requested (no driver available): {pickup} → {dropoff}")


def simulate_complete_ride(conn):
    """Complete an in-progress ride, add payment and optional rating."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r.id, r.rider_id, r.driver_id, ri.first_name, d.first_name "
            "FROM rides r JOIN riders ri ON ri.id = r.rider_id JOIN drivers d ON d.id = r.driver_id "
            "WHERE r.status = 'in_progress' ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        ride_id, rider_id, driver_id, rider_name, driver_name = row

        distance = round(random.uniform(1.5, 30.0), 2)
        fare = int(distance * random.uniform(150, 250))  # cents per km

        cur.execute(
            "UPDATE rides SET status = 'completed', distance_km = %s, fare_cents = %s, dropped_off_at = %s WHERE id = %s",
            (distance, fare, now(), ride_id),
        )
        cur.execute("UPDATE riders SET last_ride_at = %s WHERE id = %s", (now(), rider_id))

        # Payment
        method = random.choice(PAYMENT_METHODS)
        cur.execute(
            "INSERT INTO payments (ride_id, rider_id, amount_cents, method, status, charged_at) VALUES (%s, %s, %s, %s, 'charged', %s)",
            (ride_id, rider_id, fare, method, now()),
        )

        # Rating (80% chance)
        if random.random() < 0.8:
            score = random.choices([5, 4, 3, 2, 1], weights=[60, 25, 10, 3, 2])[0]
            comment = random.choice(COMMENTS)
            cur.execute(
                "INSERT INTO ratings (ride_id, from_rider, score, comment, created_at) VALUES (%s, true, %s, %s, %s)",
                (ride_id, score, comment, now()),
            )

    conn.commit()
    print(f"[complete] {rider_name}'s ride with {driver_name} — ${fare/100:.2f} ({distance}km)")


def simulate_cancel_ride(conn):
    """Cancel a requested or in-progress ride."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r.id, ri.first_name FROM rides r JOIN riders ri ON ri.id = r.rider_id "
            "WHERE r.status IN ('requested', 'in_progress') ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        ride_id, name = row
        cur.execute("UPDATE rides SET status = 'cancelled' WHERE id = %s", (ride_id,))
    conn.commit()
    print(f"[cancel] {name} cancelled a ride")


ACTIONS = [
    (simulate_new_rider, 0.10),
    (simulate_new_driver, 0.05),
    (simulate_request_ride, 0.35),
    (simulate_complete_ride, 0.35),
    (simulate_cancel_ride, 0.15),
]


def pick_action():
    r = random.random()
    cumulative = 0
    for fn, weight in ACTIONS:
        cumulative += weight
        if r < cumulative:
            return fn
    return ACTIONS[-1][0]


def main():
    parser = argparse.ArgumentParser(description="Simulate Rideshare app activity")
    parser.add_argument("--interval", type=float, default=5, help="Seconds between events (default: 5)")
    parser.add_argument("--once", action="store_true", help="Run one batch and exit")
    args = parser.parse_args()

    conn = connect()
    print(f"Connected to database. Simulating every {args.interval}s...")

    try:
        while True:
            action = pick_action()
            try:
                action(conn)
            except Exception as e:
                print(f"[error] {e}")
                conn.rollback()

            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
