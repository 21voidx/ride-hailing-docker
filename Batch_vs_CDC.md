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

---

## Jalankan

```bash
docker compose up --build
```

Adminer:

- URL: <http://localhost:8080>
- System: PostgreSQL
- Server: postgres
- Username: ride_user
- Password: ride_pass
- Database: ride_db

---

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

---

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

---

# ELT ke Star Schema: Batch vs CDC

Bagian ini menjelaskan tabel mana yang sebaiknya masuk lewat **batch ELT** dan mana yang sebaiknya masuk lewat **CDC** jika target akhirnya adalah **star schema**.

Prinsip dasarnya:

- **CDC** cocok untuk tabel transaksi dan event yang sering berubah, membutuhkan urutan waktu, dan penting untuk near real-time analytics.
- **Batch** cocok untuk lookup/reference table yang kecil, jarang berubah, dan tidak membutuhkan latency rendah.
- **Hybrid** cocok untuk master data yang bisa dibatch untuk awal, tetapi lebih aman memakai CDC jika ingin SCD Type 2, audit historis, atau dashboard operasional yang cepat.

Dalam PostgreSQL, CDC biasanya memanfaatkan logical replication atau tools seperti Debezium. Debezium menghasilkan event untuk setiap `INSERT`, `UPDATE`, dan `DELETE` row-level. PostgreSQL logical replication juga memang didesain untuk mereplikasi perubahan data berdasarkan replication identity seperti primary key.

---

## Ringkasan keputusan per tabel

| Tabel OLTP | Rekomendasi ingest | Prioritas | Alasan utama | Target umum di star schema |
|---|---|---:|---|---|
| `role` | Batch | Rendah | Lookup kecil, jarang berubah | `dim_role` atau helper security dimension |
| `payment_method_type` | Batch | Rendah | Lookup kecil, jarang berubah | `dim_payment_method_type` |
| `promotion` | Hybrid | Sedang | Master promo bisa berubah, tetapi tidak setinggi transaksi | `dim_promotion` SCD1/SCD2 |
| `user_account` | Hybrid, cenderung CDC | Tinggi | Master user berubah, soft delete, verifikasi, status akun | `dim_user`, `dim_rider` SCD1/SCD2 |
| `user_role` | CDC atau batch incremental | Sedang | Role bisa berubah dan berdampak ke segmentasi user | bridge/helper dimension |
| `driver_profile` | CDC | Tinggi | Status, verifikasi, suspend, rating summary berubah | `dim_driver` SCD2 atau current snapshot |
| `driver_document` | Batch incremental atau CDC | Sedang | Penting untuk compliance, tetapi tidak selalu dibutuhkan di mart utama | compliance mart, `dim_driver_document` |
| `vehicle` | CDC | Tinggi | Status/verifikasi kendaraan bisa berubah dan memengaruhi ride historis | `dim_vehicle` SCD2 |
| `driver_vehicle_assignment` | CDC | Tinggi | Relasi driver-kendaraan bersifat historis | bridge driver-vehicle, SCD relationship |
| `ride` | CDC wajib | Sangat tinggi | Core transaction, banyak update status dan timestamp lifecycle | `fact_ride`, `fact_ride_lifecycle` |
| `ride_status_history` | CDC wajib | Sangat tinggi | Event timeline, audit status, funnel operasional | `fact_ride_status_event` |
| `ride_location` | CDC | Tinggi | Pickup/dropoff aktual dan request location penting untuk analisis geo | `dim_location`, `fact_ride_location_event` |
| `ride_tracking_point` | CDC atau micro-batch | Tinggi, volume besar | High-volume GPS event, bisa sangat besar | `fact_ride_tracking_point`, aggregate route mart |
| `ride_fare` | CDC wajib | Sangat tinggi | Fare final/estimated/adjusted penting untuk revenue | `fact_ride_fare` atau bagian `fact_ride` |
| `ride_fare_component` | CDC | Tinggi | Breakdown fare penting untuk audit dan margin | `fact_fare_component` |
| `user_payment_method` | CDC dengan masking | Sedang-tinggi | Status payment method berubah, tapi token jangan masuk analytics mentah | `dim_user_payment_method_masked` |
| `payment_transaction` | CDC wajib | Sangat tinggi | Payment status berubah, retry, idempotency, settlement | `fact_payment_transaction` |
| `payment_refund` | CDC wajib | Sangat tinggi | Refund memengaruhi net revenue | `fact_refund` |
| `review` | CDC | Sedang-tinggi | Review bisa datang terlambat atau dihapus | `fact_review`, `dim_review_text` opsional |
| `promo_usage` | CDC wajib | Tinggi | Event pemakaian promo memengaruhi net fare dan campaign analytics | `fact_promo_usage` |

---

## Kelompok 1: Tabel yang cukup batch

Tabel ini kecil, relatif stabil, dan tidak butuh latency rendah.

```text
role
payment_method_type
```

Rekomendasi:

- Load full refresh harian atau setiap deploy.
- Bisa juga pakai snapshot kecil per jam, tetapi biasanya berlebihan.
- Gunakan `updated_at` jika ingin incremental batch.

Contoh ELT:

```sql
-- contoh pattern batch full refresh untuk lookup kecil
TRUNCATE TABLE staging.role;

INSERT INTO staging.role
SELECT *
FROM oltp.role;
```

Target star schema:

```text
dim_role
dim_payment_method_type
```

Catatan skeptis: jangan pakai CDC untuk semua lookup kecil hanya agar terlihat canggih. Itu sering menambah kompleksitas tanpa nilai analitik yang nyata.

---

## Kelompok 2: Tabel hybrid

Tabel ini bisa batch untuk MVP, tetapi lebih aman CDC jika kamu ingin histori, SCD2, atau dashboard yang lebih real-time.

```text
promotion
user_account
user_role
driver_document
```

### `promotion`

Pakai batch jika:

- Promo jarang berubah.
- Analisis promo cukup harian.
- Tidak butuh status promo real-time.

Pakai CDC jika:

- Promo bisa dinonaktifkan mendadak.
- `valid_from`, `valid_to`, `promotion_status`, atau limit promo sering berubah.
- Kamu ingin audit perubahan campaign.

Target star schema:

```text
dim_promotion
fact_promo_usage
```

### `user_account`

Pakai CDC jika kamu ingin menangkap:

```text
account_status
email_verified_at
phone_verified_at
deleted_at
last_login_at
```

Untuk star schema, jangan bawa PII mentah sembarangan. Untuk data mart analitik, cukup bawa atribut aman seperti:

```text
user_key
user_id
account_status
email_verified_flag
phone_verified_flag
created_date
is_deleted
```

Target:

```text
dim_user
dim_rider
```

### `driver_document`

Pakai batch incremental jika hanya untuk reporting compliance harian.

Pakai CDC jika:

- Verifikasi driver harus dipantau hampir real-time.
- Ada proses fraud/compliance monitoring.
- Status dokumen memengaruhi kemampuan driver menerima ride.

---

## Kelompok 3: Tabel yang sebaiknya CDC

Tabel ini adalah inti lifecycle ride-hailing. Untuk ELT dan star schema, CDC memberi urutan kejadian yang lebih aman daripada batch polling.

```text
driver_profile
vehicle
driver_vehicle_assignment
ride
ride_status_history
ride_location
ride_tracking_point
ride_fare
ride_fare_component
user_payment_method
payment_transaction
payment_refund
review
promo_usage
```

### Kenapa `ride` wajib CDC?

`ride` bukan tabel insert-only. Dalam lifecycle nyata, satu ride mengalami banyak update:

```text
REQUESTED
ACCEPTED
ARRIVED
IN_PROGRESS
COMPLETED
CANCELLED
PAYMENT_FAILED
```

Jika hanya batch harian, kamu bisa kehilangan urutan perubahan status atau melihat status akhir saja. Untuk star schema, status akhir penting, tetapi timeline status juga penting untuk funnel, SLA, cancellation, dan operasi.

Target star schema:

```text
fact_ride
fact_ride_lifecycle
fact_ride_status_event
```

### Kenapa `ride_status_history` wajib CDC?

Tabel ini adalah event log. Ia hampir selalu append-only. Ini kandidat paling bersih untuk fact event.

Target:

```text
fact_ride_status_event
```

Grain:

```text
1 row = 1 status change event for 1 ride
```

Measures/metrics turunan:

```text
minutes_requested_to_accepted
minutes_accepted_to_arrived
minutes_started_to_completed
cancel_rate
completion_rate
```

### Kenapa `payment_transaction` wajib CDC?

Payment bisa berubah status:

```text
PENDING
AUTHORIZED
CAPTURED
PAID
FAILED
REFUNDED
```

Payment juga sensitif terhadap retry dan idempotency. Jika ELT hanya batch, status antara bisa hilang. Untuk finance analytics, ini rawan.

Target:

```text
fact_payment_transaction
```

Grain:

```text
1 row = 1 payment transaction attempt
```

### Kenapa `payment_refund` wajib CDC?

Refund memengaruhi net revenue. Ia bisa terjadi setelah ride selesai, bahkan jauh setelah payment berhasil.

Target:

```text
fact_refund
```

Grain:

```text
1 row = 1 refund transaction
```

### Kenapa `ride_fare` wajib CDC?

Fare bisa estimated, final, adjusted, atau corrected. Perubahan fare memengaruhi revenue, driver earning, platform fee, tax, dan discount.

Target:

```text
fact_ride_fare
fact_revenue
```

Jika hanya satu final fare yang dianalisis, kamu tetap bisa CDC lalu ambil latest final fare di mart.

### Kenapa `ride_tracking_point` bisa CDC atau micro-batch?

`ride_tracking_point` high volume. CDC memberi detail terbaik, tetapi bisa berat.

Untuk laptop/lab ringan:

```text
micro-batch setiap 1-5 menit cukup
```

Untuk real-time operations:

```text
CDC atau streaming wajib
```

Target star schema:

```text
fact_ride_tracking_point
fact_ride_route_summary
```

Saran praktis:

- Simpan raw tracking point di staging/raw layer.
- Untuk star schema utama, buat aggregate route summary.
- Jangan jadikan semua GPS point sebagai tabel utama dashboard eksekutif.

---

# Rancangan Star Schema dari OLTP Ini

## Dimensi utama

```text
dim_date
dim_time
dim_user
dim_rider
dim_driver
dim_vehicle
dim_promotion
dim_payment_method_type
dim_city
dim_service_type
dim_location
```

## Fact utama

```text
fact_ride
fact_ride_status_event
fact_ride_fare
fact_fare_component
fact_payment_transaction
fact_refund
fact_promo_usage
fact_review
fact_ride_tracking_point
fact_ride_route_summary
```

---

## Mapping OLTP ke Star Schema

| Source table | Star schema target | Grain target |
|---|---|---|
| `ride` | `fact_ride` | 1 row = 1 ride |
| `ride_status_history` | `fact_ride_status_event` | 1 row = 1 ride status change |
| `ride_location` | `fact_ride_location_event` atau `dim_location` | 1 row = 1 ride location record |
| `ride_tracking_point` | `fact_ride_tracking_point` | 1 row = 1 GPS point |
| `ride_tracking_point` | `fact_ride_route_summary` | 1 row = 1 ride route summary |
| `ride_fare` | `fact_ride_fare` | 1 row = 1 fare version per ride |
| `ride_fare_component` | `fact_fare_component` | 1 row = 1 fare component |
| `payment_transaction` | `fact_payment_transaction` | 1 row = 1 payment attempt/transaction |
| `payment_refund` | `fact_refund` | 1 row = 1 refund |
| `promo_usage` | `fact_promo_usage` | 1 row = 1 promo usage event |
| `review` | `fact_review` | 1 row = 1 review |
| `user_account` | `dim_user`, `dim_rider` | 1 row = 1 user version or current user |
| `driver_profile` | `dim_driver` | 1 row = 1 driver version or current driver |
| `vehicle` | `dim_vehicle` | 1 row = 1 vehicle version or current vehicle |
| `driver_vehicle_assignment` | bridge or helper fact | 1 row = 1 driver-vehicle assignment period |
| `promotion` | `dim_promotion` | 1 row = 1 promotion version or current promotion |
| `payment_method_type` | `dim_payment_method_type` | 1 row = 1 payment method type |
| `role`, `user_role` | optional security mart | depends on analysis need |

---

# Recommended ELT Flow

## MVP ringan

Gunakan batch incremental untuk semua tabel, kecuali event utama yang disimulasikan dengan generator.

```text
PostgreSQL OLTP
  -> staging schema
  -> transform SQL/dbt
  -> star schema
```

Cocok jika tujuanmu:

- belajar star schema
- membuat dashboard batch
- belum perlu streaming/CDC stack

## Recommended production-like lab

Gunakan campuran batch dan CDC.

```text
PostgreSQL OLTP
  -> CDC raw changes for transaction/event tables
  -> batch snapshots for lookup/master tables
  -> staging normalized layer
  -> star schema marts
```

Tabel CDC prioritas pertama:

```text
ride
ride_status_history
ride_fare
payment_transaction
payment_refund
promo_usage
```

Tabel CDC prioritas kedua:

```text
driver_profile
vehicle
driver_vehicle_assignment
ride_location
review
user_payment_method
```

Tabel batch:

```text
role
payment_method_type
promotion untuk MVP
user_account untuk MVP non-SCD2
```

---

# Suggested Pipeline Schedule

| Pipeline | Mode | Frequency | Isi |
|---|---|---:|---|
| Reference batch | Batch | 1x per hari | `role`, `payment_method_type` |
| Master snapshot | Batch/CDC | 15 menit - harian | `user_account`, `driver_profile`, `vehicle`, `promotion` |
| Ride event ingest | CDC | real-time | `ride`, `ride_status_history` |
| Payment ingest | CDC | real-time | `payment_transaction`, `payment_refund` |
| Fare ingest | CDC | real-time | `ride_fare`, `ride_fare_component` |
| Promo usage ingest | CDC | real-time | `promo_usage` |
| Tracking ingest | CDC/micro-batch | 1-5 menit | `ride_tracking_point` |
| Mart build | Batch incremental | 5-15 menit | star schema facts and dimensions |

---

# Praktik Aman untuk ELT

## 1. Jangan transform langsung dari OLTP ke mart

Gunakan minimal tiga layer:

```text
raw/staging
clean/integration
mart/star
```

## 2. Gunakan timestamp bisnis, bukan load time

Untuk fact, prioritaskan business event time:

```text
requested_at
accepted_at
arrived_at
started_at
completed_at
paid_at
refunded_at
used_at
created_at
```

`load_datetime` tetap penting, tetapi untuk audit pipeline.

## 3. Jangan jadikan `updated_at` sebagai satu-satunya CDC palsu

Batch incremental dengan `updated_at` bisa cukup untuk MVP. Tapi ia bisa gagal jika:

- update tidak mengubah `updated_at`
- delete fisik terjadi
- ada multiple update dalam window yang sama
- kamu butuh urutan perubahan status

Untuk event lifecycle dan payment, CDC lebih aman.

## 4. Pisahkan PII dari analytics mart

Kolom seperti berikut sebaiknya tidak masuk mart umum:

```text
email
phone_number
password_hash
provider_payment_token
document_file_url
```

Gunakan masking atau exclude dari mart.

## 5. Tentukan grain sebelum transform

Contoh grain aman:

```text
fact_ride: 1 row = 1 ride
fact_ride_status_event: 1 row = 1 status change event
fact_payment_transaction: 1 row = 1 payment transaction attempt
fact_refund: 1 row = 1 refund transaction
fact_ride_tracking_point: 1 row = 1 GPS point
fact_ride_route_summary: 1 row = 1 ride
```

---

# Keputusan Praktis untuk Project Ini

Untuk project Docker Compose ini, mulai dengan mode berikut:

```text
Batch:
- role
- payment_method_type
- promotion

CDC wajib / simulasi realtime:
- ride
- ride_status_history
- ride_fare
- ride_fare_component
- payment_transaction
- payment_refund
- promo_usage

CDC atau micro-batch:
- ride_location
- ride_tracking_point
- review
- driver_profile
- vehicle
- driver_vehicle_assignment
- user_payment_method

Hybrid:
- user_account
- user_role
- driver_document
```

Jika tujuan utama adalah belajar star schema, jangan langsung terlalu banyak CDC. Mulai dari CDC untuk `ride`, `ride_status_history`, `ride_fare`, dan `payment_transaction`. Setelah mart stabil, baru tambahkan refund, promo, review, dan tracking.

---

# Sumber rujukan singkat

- Debezium PostgreSQL connector menghasilkan event perubahan untuk setiap row-level `INSERT`, `UPDATE`, dan `DELETE`.
- PostgreSQL logical replication mereplikasi perubahan data berdasarkan replication identity, biasanya primary key.
- Untuk dimensional modeling, fact table perlu grain yang jelas. Event seperti status change, payment transaction, refund, dan tracking point lebih cocok menjadi fact event daripada digabung sembarangan ke satu fact besar.
