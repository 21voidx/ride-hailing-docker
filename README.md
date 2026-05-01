# Ride-Hailing Realtime CRUD Generator (Lightweight)

Template ini menjalankan:

- PostgreSQL 16
- Python realtime CRUD generator
- Adminer UI

Generator membuat lifecycle ride-hailing yang realistis:

1. `REQUESTED`
2. `ACCEPTED`
3. `ARRIVED`
4. `IN_PROGRESS`
5. tracking point selama perjalanan
6. `COMPLETED` atau `CANCELLED` atau `PAYMENT_FAILED`
7. fare final
8. payment transaction
9. review opsional

Timestamp di database tidak memakai `now()` mentah untuk semua event. Event time dibuat realistis:
- accepted beberapa menit setelah requested
- arrived beberapa menit setelah accepted
- started setelah rider naik
- completed puluhan menit setelah started
- payment beberapa menit setelah completed
- review bisa terlambat menit sampai jam

Untuk demo ringan, waktu nyata dipercepat lewat `SIM_SECONDS_PER_MINUTE`.

## Jalankan

```bash
docker compose up --build
```

Adminer:

- URL: http://localhost:8080
- System: PostgreSQL
- Server: postgres
- Username: ride_user
- Password: ride_pass
- Database: ride_db

## Query cepat

```sql
SELECT ride_status, count(*)
FROM ride
GROUP BY ride_status
ORDER BY ride_status;
```

```sql
SELECT
  ride_id,
  ride_status,
  requested_at,
  accepted_at,
  arrived_at,
  started_at,
  completed_at,
  EXTRACT(EPOCH FROM (completed_at - requested_at))/60 AS request_to_complete_minutes
FROM ride
ORDER BY ride_id DESC
LIMIT 20;
```

```sql
SELECT
  r.ride_id,
  r.requested_at,
  r.completed_at,
  f.total_fare,
  p.payment_status
FROM ride r
LEFT JOIN ride_fare f ON r.ride_id = f.ride_id
LEFT JOIN payment_transaction p ON r.ride_id = p.ride_id
ORDER BY r.ride_id DESC
LIMIT 20;
```

```sql
SELECT ride_id, count(*) AS tracking_points
FROM ride_tracking_point
GROUP BY ride_id
ORDER BY ride_id DESC
LIMIT 20;
```

## Kontrol beban

Edit environment di `docker-compose.yml`:

```yaml
RIDES_PER_MINUTE: 4
MAX_CONCURRENT_RIDES: 12
SIM_SECONDS_PER_MINUTE: 0.12
TRACKING_INTERVAL_SIM_MINUTES: 3
MAX_TRACKING_POINTS_PER_RIDE: 24
```

Untuk laptop 8 GB RAM, jangan langsung naikkan `RIDES_PER_MINUTE` dan tracking terlalu tinggi.
