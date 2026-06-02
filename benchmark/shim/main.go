// pg2iceberg benchmark HTTP shim.
//
// Stateless service that translates HTTP calls into the same
// INSERT/UPDATE/DELETE mix the rideshare `example/single/simulate.py`
// performs, so the load generator (k6) needs no database driver.
//
// It also owns the control plane for the benchmark:
//   POST /admin/migrate   create schema (idempotent)
//   POST /admin/seed      bulk-load fake data via generate_series
//   GET  /admin/lag       replication slot lag + coord pending events + row counts
//
// Traffic endpoints (each = one weighted action from simulate.py):
//   POST /riders          new rider
//   POST /drivers         new driver
//   POST /rides/request   request + match a ride
//   POST /rides/complete  complete ride + payment + (80%) rating
//   POST /rides/cancel    cancel a requested/in-progress ride
//   GET  /healthz         ALB health check
//
// Config (env):
//   POSTGRES_URL   postgres://user:pass@host:5432/db?sslmode=require  (required)
//   PORT           listen port (default 8080)
//   COORD_SCHEMA   pg2iceberg coordinator schema (default _pg2iceberg)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	firstNames = []string{"Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason", "Mia", "Lucas", "Harper", "Aiden", "Ella", "Caden", "Aria", "Logan", "Riley", "Zara", "Leo", "Nora", "Kai"}
	lastNames  = []string{"Smith", "Park", "Garcia", "Nakamura", "Brown", "Singh", "Muller", "Costa", "Ali", "OBrien", "Kowalski", "Fernandez", "Sato", "Williams", "Kim", "Chen", "Johansson", "Ahmed", "Taylor", "Rossi"}
	cities     = []string{"San Francisco", "New York", "Austin", "Los Angeles", "Chicago", "Seattle"}
	addresses  = []string{"123 Market St", "456 Mission St", "789 Valencia St", "100 Embarcadero", "SFO Airport", "500 Castro St", "350 5th Ave", "1 Penn Plaza", "JFK Airport", "1100 Congress Ave", "The Domain", "Zilker Park"}
	makes      = []string{"Toyota", "Honda", "Tesla", "Hyundai", "Ford", "Kia", "BMW", "Chevrolet", "Nissan"}
	models     = []string{"Camry", "Civic", "Model 3", "Ioniq 5", "Mach-E", "EV6", "i4", "Bolt", "Leaf"}
	methods    = []string{"card", "card", "card", "wallet", "wallet"}
	comments   = []string{"", "", "Great ride!", "Smooth driving", "Quick pickup", "Very professional", "Love the EV!", "Friendly driver", "Clean car"}
)

func pick(s []string) string { return s[rand.Intn(len(s))] }

type shim struct {
	pool        *pgxpool.Pool
	coordSchema string
	maxRider    atomic.Int64
	maxDriver   atomic.Int64
}

func atomicMax(a *atomic.Int64, v int64) {
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

func main() {
	dsn := os.Getenv("POSTGRES_URL")
	if dsn == "" {
		log.Fatal("POSTGRES_URL is required")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	coord := os.Getenv("COORD_SCHEMA")
	if coord == "" {
		coord = "_pg2iceberg"
	}

	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("parse POSTGRES_URL: %v", err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	s := &shim{pool: pool, coordSchema: coord}
	s.refreshMaxIDs(ctx) // best-effort; tables may not exist yet

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /admin/migrate", s.handleMigrate)
	mux.HandleFunc("POST /admin/seed", s.handleSeed)
	mux.HandleFunc("GET /admin/lag", s.handleLag)
	mux.HandleFunc("POST /riders", s.handleNewRider)
	mux.HandleFunc("POST /drivers", s.handleNewDriver)
	mux.HandleFunc("POST /rides/request", s.handleRequestRide)
	mux.HandleFunc("POST /rides/complete", s.handleCompleteRide)
	mux.HandleFunc("POST /rides/cancel", s.handleCancelRide)

	log.Printf("shim listening on :%s (coord schema %s)", port, coord)
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadTimeout: 30 * time.Second, WriteTimeout: 5 * time.Minute}
	log.Fatal(srv.ListenAndServe())
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func httpErr(w http.ResponseWriter, err error) {
	log.Printf("error: %v", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}

func (s *shim) refreshMaxIDs(ctx context.Context) {
	var n int64
	if err := s.pool.QueryRow(ctx, "SELECT COALESCE(MAX(id),0) FROM riders").Scan(&n); err == nil {
		s.maxRider.Store(n)
	}
	if err := s.pool.QueryRow(ctx, "SELECT COALESCE(MAX(id),0) FROM drivers").Scan(&n); err == nil {
		s.maxDriver.Store(n)
	}
}

// randomRider picks a (mostly) uniform random rider by id range. riders
// are insert-only so ids are dense — an index range scan, not ORDER BY
// random(), keeps Postgres from becoming the benchmark bottleneck.
func (s *shim) randomRider(ctx context.Context) (id int, city string, ok bool) {
	max := s.maxRider.Load()
	if max <= 0 {
		return 0, "", false
	}
	n := rand.Int63n(max) + 1
	err := s.pool.QueryRow(ctx, "SELECT id, city FROM riders WHERE id >= $1 ORDER BY id LIMIT 1", n).Scan(&id, &city)
	if err != nil {
		if err = s.pool.QueryRow(ctx, "SELECT id, city FROM riders ORDER BY id LIMIT 1").Scan(&id, &city); err != nil {
			return 0, "", false
		}
	}
	return id, city, true
}

// ── traffic handlers ────────────────────────────────────────────────

func (s *shim) handleNewRider(w http.ResponseWriter, r *http.Request) {
	first, last, city := pick(firstNames), pick(lastNames), pick(cities)
	email := fmt.Sprintf("rider.%s.%s.%d@bench.local", first, last, rand.Intn(1_000_000))
	var id int64
	err := s.pool.QueryRow(r.Context(),
		`INSERT INTO riders (email, first_name, last_name, phone, city, signed_up_at)
		 VALUES ($1,$2,$3,$4,$5, now()) RETURNING id`,
		email, first, last, fmt.Sprintf("+1-555-%04d", rand.Intn(10000)), city).Scan(&id)
	if err != nil {
		httpErr(w, err)
		return
	}
	atomicMax(&s.maxRider, id)
	writeJSON(w, http.StatusOK, map[string]any{"rider_id": id})
}

func (s *shim) handleNewDriver(w http.ResponseWriter, r *http.Request) {
	first, last, city := pick(firstNames), pick(lastNames), pick(cities)
	mk, md := pick(makes), pick(models)
	plate := fmt.Sprintf("%d%c%c%c%03d", rand.Intn(9)+1, 'A'+rune(rand.Intn(26)), 'A'+rune(rand.Intn(26)), 'A'+rune(rand.Intn(26)), rand.Intn(1000))
	email := fmt.Sprintf("driver.%s.%d@bench.local", first, rand.Intn(1_000_000))
	var id int64
	err := s.pool.QueryRow(r.Context(),
		`INSERT INTO drivers (email, first_name, last_name, phone, city, vehicle_make, vehicle_model, vehicle_year, license_plate, signed_up_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now()) RETURNING id`,
		email, first, last, fmt.Sprintf("+1-555-%04d", rand.Intn(10000)), city, mk, md, 2023+rand.Intn(3), plate).Scan(&id)
	if err != nil {
		httpErr(w, err)
		return
	}
	atomicMax(&s.maxDriver, id)
	writeJSON(w, http.StatusOK, map[string]any{"driver_id": id})
}

func (s *shim) handleRequestRide(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	riderID, city, ok := s.randomRider(ctx)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"skipped": "no riders"})
		return
	}
	pickup, dropoff := pick(addresses), pick(addresses)

	var driverID int64
	derr := s.pool.QueryRow(ctx,
		"SELECT id FROM drivers WHERE city=$1 AND status='active' ORDER BY random() LIMIT 1", city).Scan(&driverID)

	var rideID int64
	if derr == nil {
		if err := s.pool.QueryRow(ctx,
			`INSERT INTO rides (rider_id, driver_id, status, pickup_address, dropoff_address, requested_at, picked_up_at)
			 VALUES ($1,$2,'in_progress',$3,$4, now(), now() + (interval '1 minute' * (3+floor(random()*9)))) RETURNING id`,
			riderID, driverID, pickup, dropoff).Scan(&rideID); err != nil {
			httpErr(w, err)
			return
		}
	} else {
		if err := s.pool.QueryRow(ctx,
			`INSERT INTO rides (rider_id, status, pickup_address, dropoff_address, requested_at)
			 VALUES ($1,'requested',$2,$3, now()) RETURNING id`,
			riderID, pickup, dropoff).Scan(&rideID); err != nil {
			httpErr(w, err)
			return
		}
	}
	s.pool.Exec(ctx, "UPDATE riders SET last_ride_at = now() WHERE id = $1", riderID)
	writeJSON(w, http.StatusOK, map[string]any{"ride_id": rideID})
}

func (s *shim) handleCompleteRide(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var rideID, riderID int64
	err := s.pool.QueryRow(ctx,
		"SELECT id, rider_id FROM rides WHERE status='in_progress' ORDER BY random() LIMIT 1").Scan(&rideID, &riderID)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"skipped": "no in_progress rides"})
		return
	}
	distance := 1.5 + rand.Float64()*28.5
	fare := int(distance * (150 + rand.Float64()*100))

	if _, err := s.pool.Exec(ctx,
		"UPDATE rides SET status='completed', distance_km=$1, fare_cents=$2, dropped_off_at=now() WHERE id=$3",
		fmt.Sprintf("%.2f", distance), fare, rideID); err != nil {
		httpErr(w, err)
		return
	}
	s.pool.Exec(ctx, "UPDATE riders SET last_ride_at = now() WHERE id = $1", riderID)
	s.pool.Exec(ctx,
		"INSERT INTO payments (ride_id, rider_id, amount_cents, method, status, charged_at) VALUES ($1,$2,$3,$4,'charged',now())",
		rideID, riderID, fare, pick(methods))
	if rand.Float64() < 0.8 {
		score := []int{5, 5, 5, 4, 4, 3, 2, 1}[rand.Intn(8)]
		s.pool.Exec(ctx,
			"INSERT INTO ratings (ride_id, from_rider, score, comment, created_at) VALUES ($1, true, $2, NULLIF($3,''), now())",
			rideID, score, pick(comments))
	}
	writeJSON(w, http.StatusOK, map[string]any{"ride_id": rideID, "fare_cents": fare})
}

func (s *shim) handleCancelRide(w http.ResponseWriter, r *http.Request) {
	ct, err := s.pool.Exec(r.Context(),
		`UPDATE rides SET status='cancelled'
		 WHERE id = (SELECT id FROM rides WHERE status IN ('requested','in_progress') ORDER BY random() LIMIT 1)`)
	if err != nil {
		httpErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cancelled": ct.RowsAffected()})
}

// ── admin handlers ──────────────────────────────────────────────────

func (s *shim) handleMigrate(w http.ResponseWriter, r *http.Request) {
	if _, err := s.pool.Exec(r.Context(), schemaDDL); err != nil {
		httpErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"migrated": true})
}

type seedReq struct {
	Riders  int `json:"riders"`
	Drivers int `json:"drivers"`
	Rides   int `json:"rides"`
}

func (s *shim) handleSeed(w http.ResponseWriter, r *http.Request) {
	req := seedReq{Riders: 50000, Drivers: 5000, Rides: 200000}
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}
	ctx := r.Context()
	// One transaction so a partial failure rolls back cleanly. TRUNCATE
	// first makes reseeds idempotent (safe: pg2iceberg isn't running
	// yet during seeding).
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		httpErr(w, err)
		return
	}
	defer tx.Rollback(ctx)
	for _, step := range []struct {
		sql  string
		args []any
	}{
		{`TRUNCATE ratings, payments, rides, drivers, riders RESTART IDENTITY CASCADE`, nil},
		{seedRiders, []any{req.Riders}},
		{seedDrivers, []any{req.Drivers}},
		{seedRides, []any{req.Riders, req.Drivers, req.Rides}},
		{seedPayments, nil},
		{seedRatings, nil},
	} {
		if _, err := tx.Exec(ctx, step.sql, step.args...); err != nil {
			httpErr(w, err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		httpErr(w, err)
		return
	}
	s.refreshMaxIDs(ctx)
	writeJSON(w, http.StatusOK, map[string]any{"seeded": req})
}

func (s *shim) handleLag(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	out := map[string]any{}

	// Replication slot lag (bytes the flusher is behind the WAL head).
	var lagBytes int64
	var walStatus string
	slotErr := s.pool.QueryRow(ctx,
		`SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),0)::bigint,
		        COALESCE(wal_status,'unknown')
		 FROM pg_replication_slots WHERE slot_name = 'pg2iceberg_slot'`).Scan(&lagBytes, &walStatus)
	if slotErr != nil {
		out["slot"] = map[string]any{"present": false}
	} else {
		out["slot"] = map[string]any{"present": true, "lag_bytes": lagBytes, "wal_status": walStatus}
	}

	// Flush→materialize lag: events staged but not yet materialized.
	// Guarded with to_regclass so it works before pg2iceberg first runs.
	var pending int64 = -1
	q := fmt.Sprintf(
		`SELECT COALESCE(SUM(GREATEST(sq.next_offset - 1 - COALESCE(c.last_offset,-1), 0)),0)::bigint
		 FROM %s.log_seq sq
		 LEFT JOIN %s.mat_cursor c ON c.table_name = sq.table_name AND c.group_name = 'default'`,
		s.coordSchema, s.coordSchema)
	var reg *string
	if s.pool.QueryRow(ctx, fmt.Sprintf("SELECT to_regclass('%s.log_seq')::text", s.coordSchema)).Scan(&reg) == nil && reg != nil {
		s.pool.QueryRow(ctx, q).Scan(&pending)
	}
	out["pending_events"] = pending

	// Per-table row counts (cheap correctness cross-check).
	rows := map[string]int64{}
	for _, t := range []string{"riders", "drivers", "rides", "payments", "ratings"} {
		var n int64
		if s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+t).Scan(&n) == nil {
			rows[t] = n
		}
	}
	out["rows"] = rows
	out["ts"] = time.Now().UTC().Format(time.RFC3339)
	writeJSON(w, http.StatusOK, out)
}
