# Ride-Hailing Realtime CRUD Generator: Batch vs CDC untuk ELT Postgres ke BigQuery

Dokumen ini disesuaikan dengan source PostgreSQL terbaru pada `init_postgres(1).sql` dan perilaku generator terbaru pada `generator(1).py`. Fokus dokumen ini adalah menentukan tabel mana yang lebih tepat diproses melalui batch, CDC, atau hybrid ketika data dari PostgreSQL akan dimuat ke data warehouse BigQuery melalui Trino.

Arsitektur yang diasumsikan:

```text
PostgreSQL OLTP
  -> Trino sebagai query/federation layer
  -> BigQuery raw/staging
  -> BigQuery clean/integration
  -> BigQuery mart/star schema
```

Catatan penting: walaupun Trino dapat membantu membaca dan memindahkan data lintas sumber, desain fisik tabel BigQuery seperti partition dan cluster tetap sebaiknya ditentukan berdasarkan pola query, grain fact, dan kebutuhan retensi data warehouse.

---

## 1. Ringkasan source terbaru

Template ini menjalankan komponen utama berikut:

- PostgreSQL 16 sebagai sumber OLTP.
- Python realtime CRUD generator.
- Adminer UI untuk inspeksi data.

Generator membentuk lifecycle ride-hailing yang realistis:

1. `REQUESTED`
2. `ACCEPTED`
3. `ARRIVED`
4. `IN_PROGRESS`
5. tracking point selama perjalanan
6. `COMPLETED`, `CANCELLED`, atau `PAYMENT_FAILED`
7. fare final
8. payment transaction
9. review opsional

Timestamp pada database dibuat sebagai event time, bukan sekadar `now()` untuk semua baris. Generator membuat urutan waktu yang lebih realistis, misalnya accepted terjadi beberapa menit setelah requested, arrived setelah accepted, started setelah rider naik, completed setelah durasi perjalanan, payment beberapa menit setelah completed, dan review bisa muncul terlambat.

Untuk demo ringan, waktu simulasi dipercepat lewat `SIM_SECONDS_PER_MINUTE`.

---

## 2. Kontrol beban generator

Edit environment pada `docker-compose.yml` atau `.env`:

```yaml
RIDES_PER_MINUTE: 4
MAX_CONCURRENT_RIDES: 12
SIM_SECONDS_PER_MINUTE: 0.12
TRACKING_INTERVAL_SIM_MINUTES: 3
MAX_TRACKING_POINTS_PER_RIDE: 24
```

Jika laptop terbatas, jangan langsung menaikkan `RIDES_PER_MINUTE`, `MAX_CONCURRENT_RIDES`, dan jumlah tracking point. Tabel yang paling cepat membesar biasanya `ride_tracking_point` karena satu ride bisa menghasilkan beberapa titik GPS.

Konfigurasi aman untuk laptop 8 GB RAM:

```yaml
RIDES_PER_MINUTE: 1
MAX_CONCURRENT_RIDES: 3
SIM_SECONDS_PER_MINUTE: 0.5
TRACKING_INTERVAL_SIM_MINUTES: 10
MAX_TRACKING_POINTS_PER_RIDE: 5
```

---

## 3. Query inspeksi cepat

Cek distribusi status ride:

```sql
SELECT ride_status, count(*)
FROM ride
GROUP BY ride_status
ORDER BY ride_status;
```

Cek lifecycle ride terbaru:

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

Cek fare dan payment:

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

Cek jumlah GPS point per ride:

```sql
SELECT ride_id, count(*) AS tracking_points
FROM ride_tracking_point
GROUP BY ride_id
ORDER BY ride_id DESC
LIMIT 20;
```

Cek tabel yang ada datanya tetapi tidak aktif di generator saat ini:

```sql
SELECT count(*) AS refund_count
FROM payment_refund;
```

Pada generator saat ini, `payment_refund` sudah tersedia di schema tetapi belum dibuat otomatis oleh generator. Jadi tabel ini siap untuk skenario produksi, tetapi belum menjadi sumber event utama dalam simulasi saat ini.

---

# 4. Prinsip Batch, CDC, dan Hybrid

## 4.1 Batch

Batch cocok untuk tabel kecil, jarang berubah, dan tidak membutuhkan latency rendah. Contohnya lookup table seperti `role` dan `payment_method_type`.

Karakter tabel batch:

- ukuran kecil;
- perubahan jarang;
- tidak memerlukan urutan perubahan row-level;
- jika terlambat beberapa jam atau satu hari tidak merusak analitik;
- cocok untuk full refresh atau incremental berdasarkan `updated_at`.

## 4.2 CDC

CDC cocok untuk tabel yang sering mengalami `INSERT`, `UPDATE`, dan `DELETE`, terutama tabel transaksi dan event. Untuk pipeline produksi, CDC lebih aman daripada batch polling karena perubahan status dan urutan event dapat ditangkap lebih detail.

CDC paling tepat untuk:

- core transaction;
- lifecycle event;
- status yang berubah berulang;
- payment dan revenue;
- event yang datang terlambat;
- tabel yang membutuhkan audit historis.

## 4.3 Hybrid

Hybrid cocok untuk master data. Pada tahap awal, tabel bisa diload secara batch, tetapi jika ingin histori perubahan atau SCD Type 2, tabel tersebut bisa dinaikkan menjadi CDC.

Contohnya:

- `user_account`
- `driver_profile`
- `vehicle`
- `promotion`
- `driver_vehicle_assignment`

---

# 5. Ringkasan keputusan per tabel source terbaru

| Tabel PostgreSQL | Perilaku di generator saat ini | Rekomendasi ingest lab | Rekomendasi produksi | Prioritas | Target star schema |
|---|---|---|---|---:|---|
| `role` | Seed awal, statis | Batch full refresh | Batch | Rendah | `dim_role` atau helper security dimension |
| `payment_method_type` | Seed awal, statis | Batch full refresh | Batch | Rendah | `dim_payment_method_type` |
| `promotion` | Seed awal, dipakai opsional saat promo usage | Batch incremental | Hybrid atau CDC jika campaign sering berubah | Sedang | `dim_promotion` |
| `user_account` | Seed rider, driver, admin | Batch incremental | Hybrid atau CDC untuk SCD user | Sedang | `dim_user`, `dim_rider` |
| `user_role` | Seed relasi role user | Batch incremental | Hybrid | Rendah sedang | `bridge_user_role` |
| `driver_profile` | Seed driver, lalu `driver_status` berubah `AVAILABLE` dan `ON_RIDE` | CDC atau batch pendek | CDC | Tinggi | `dim_driver_current`, `fact_driver_status_snapshot` |
| `driver_document` | Seed dokumen driver | Batch incremental | Hybrid untuk compliance | Sedang | `dim_driver_document`, compliance mart |
| `vehicle` | Seed kendaraan, relatif statis di generator | Batch incremental | Hybrid atau CDC jika status kendaraan berubah | Sedang | `dim_vehicle` |
| `driver_vehicle_assignment` | Seed relasi driver kendaraan | Batch incremental | CDC jika assignment historis penting | Sedang | `bridge_driver_vehicle_assignment` |
| `ride` | Insert `REQUESTED`, lalu banyak update status dan timestamp | CDC wajib | CDC wajib | Sangat tinggi | `fact_ride_current`, `fact_ride` |
| `ride_status_history` | Insert setiap perubahan status | CDC wajib | CDC wajib | Sangat tinggi | `fact_ride_status_event` |
| `ride_location` | Insert pickup/dropoff requested dan actual | CDC atau micro-batch | CDC | Tinggi | `fact_ride_location_event`, `dim_location` |
| `ride_tracking_point` | Insert banyak GPS point saat ride berjalan | Micro-batch atau CDC terbatas | CDC/streaming atau micro-batch sesuai SLA | Tinggi volume besar | `fact_ride_tracking_point`, `fact_ride_route_summary` |
| `ride_fare` | Insert fare final setelah ride selesai | CDC wajib | CDC wajib | Sangat tinggi | `fact_ride_fare`, `fact_revenue` |
| `ride_fare_component` | Insert breakdown fare | CDC | CDC | Tinggi | `fact_fare_component` |
| `user_payment_method` | Seed metode bayar user | Batch incremental dengan masking | CDC dengan masking | Sedang | `dim_user_payment_method_masked` |
| `payment_transaction` | Insert payment PAID/FAILED setelah ride selesai | CDC wajib | CDC wajib | Sangat tinggi | `fact_payment_transaction` |
| `payment_refund` | Ada di schema, belum dibuat generator | Tidak wajib untuk lab saat ini | CDC jika refund diaktifkan | Opsional produksi | `fact_refund` |
| `review` | Insert opsional setelah ride selesai | CDC atau micro-batch | CDC | Sedang | `fact_review` |
| `promo_usage` | Insert jika promo dipakai | CDC atau micro-batch | CDC | Tinggi | `fact_promo_usage` |

Keputusan penting: pada dokumen lama, `payment_refund` dianggap CDC wajib. Setelah source terbaru dicek, tabel ini tetap perlu disiapkan untuk produksi, tetapi untuk lab saat ini tidak wajib dimasukkan ke pipeline utama karena generator belum mengisi refund.

---

# 6. Kelompok ingest yang disarankan

## 6.1 Batch penuh untuk lookup kecil

Tabel:

```text
role
payment_method_type
```

Alasan:

- data kecil;
- hanya seed awal;
- tidak menjadi pusat analisis real-time;
- lebih sederhana jika direfresh penuh.

Contoh pola batch:

```sql
TRUNCATE TABLE raw_postgres.role;

INSERT INTO raw_postgres.role
SELECT *
FROM postgres.public.role;
```

Di BigQuery, tabel ini tidak perlu partition karena ukurannya kecil.

## 6.2 Batch incremental untuk master data statis di lab

Tabel:

```text
promotion
user_account
user_role
driver_document
vehicle
driver_vehicle_assignment
user_payment_method
```

Pada lab saat ini, tabel-tabel ini sebagian besar muncul dari seed awal. Karena itu, batch incremental berdasarkan `updated_at` sudah cukup untuk pembelajaran ELT dan star schema.

Contoh pola incremental:

```sql
SELECT *
FROM postgres.public.vehicle
WHERE updated_at >= TIMESTAMP '2026-05-01 00:00:00';
```

Catatan: untuk produksi, `vehicle`, `driver_vehicle_assignment`, dan `user_payment_method` bisa dinaikkan menjadi CDC jika perubahan historisnya penting.

## 6.3 CDC wajib untuk core transaction dan lifecycle

Tabel:

```text
ride
ride_status_history
ride_fare
ride_fare_component
payment_transaction
```

Alasan:

- `ride` mengalami banyak update status;
- `ride_status_history` adalah event log;
- `ride_fare` dan `ride_fare_component` memengaruhi revenue;
- `payment_transaction` menentukan paid, failed, dan status transaksi finansial.

Untuk tabel-tabel ini, CDC lebih cocok dibanding batch karena batch harian dapat kehilangan urutan perubahan atau hanya melihat status akhir.

## 6.4 CDC atau micro-batch untuk event besar

Tabel:

```text
ride_location
ride_tracking_point
review
promo_usage
```

Untuk lab, micro-batch setiap 1 sampai 5 menit sudah cukup. Untuk produksi dengan kebutuhan operasional real-time, CDC atau streaming lebih sesuai.

Khusus `ride_tracking_point`, jangan langsung menjadikannya tabel utama dashboard eksekutif. Simpan detail GPS di raw/staging, lalu buat ringkasan route per ride untuk mart.

## 6.5 Opsional produksi

Tabel:

```text
payment_refund
```

Tabel ini ada di schema, tetapi tidak otomatis diisi oleh generator saat ini. Jika nanti generator atau aplikasi sudah membuat refund, tabel ini sebaiknya masuk CDC karena refund memengaruhi net revenue dan bisa datang terlambat setelah transaksi utama selesai.

---

# 7. Star schema yang disarankan

## 7.1 Dimensi utama

```text
dim_date
dim_time
dim_user
dim_rider
dim_driver_current
dim_vehicle
dim_promotion
dim_payment_method_type
dim_city
dim_service_type
dim_location
```

## 7.2 Bridge dan helper

```text
bridge_user_role
bridge_driver_vehicle_assignment
```

## 7.3 Fact utama

```text
fact_ride
fact_ride_status_event
fact_ride_location_event
fact_ride_tracking_point
fact_ride_route_summary
fact_ride_fare
fact_fare_component
fact_payment_transaction
fact_promo_usage
fact_review
fact_refund
```

Catatan: `fact_refund` tetap disiapkan sebagai target produksi, tetapi tidak wajib dipakai dalam demo selama `payment_refund` belum berisi data.

---

# 8. Mapping source ke star schema

| Source table | Target star schema | Grain target | Keterangan |
|---|---|---|---|
| `ride` | `fact_ride` | 1 row = 1 ride | Simpan status akhir dan timestamp lifecycle utama |
| `ride_status_history` | `fact_ride_status_event` | 1 row = 1 status change | Cocok untuk funnel dan SLA |
| `ride_location` | `fact_ride_location_event` | 1 row = 1 location event per ride | Pickup/dropoff requested dan actual |
| `ride_tracking_point` | `fact_ride_tracking_point` | 1 row = 1 GPS point | Tabel volume besar |
| `ride_tracking_point` | `fact_ride_route_summary` | 1 row = 1 ride | Ringkasan route untuk dashboard |
| `ride_fare` | `fact_ride_fare` | 1 row = 1 fare version per ride | Saat ini generator membuat fare final |
| `ride_fare_component` | `fact_fare_component` | 1 row = 1 fare component | Breakdown base, distance, time, surge, discount, tax, platform fee |
| `payment_transaction` | `fact_payment_transaction` | 1 row = 1 payment attempt | Saat ini generator mengisi PAID atau FAILED |
| `payment_refund` | `fact_refund` | 1 row = 1 refund | Disiapkan untuk produksi, belum aktif di generator |
| `promo_usage` | `fact_promo_usage` | 1 row = 1 promo usage | Conditional jika promo dipakai |
| `review` | `fact_review` | 1 row = 1 review | Review opsional dan bisa late-arriving |
| `user_account` | `dim_user`, `dim_rider` | 1 row = 1 user current/version | Jangan bawa PII mentah ke mart umum |
| `driver_profile` | `dim_driver_current`, snapshot/fact status | 1 row = 1 driver atau 1 status snapshot | `driver_status` berubah saat ride |
| `vehicle` | `dim_vehicle` | 1 row = 1 vehicle current/version | Untuk produksi dapat dibuat SCD |
| `driver_vehicle_assignment` | `bridge_driver_vehicle_assignment` | 1 row = 1 assignment period | Penting untuk histori relasi driver kendaraan |
| `promotion` | `dim_promotion` | 1 row = 1 promotion current/version | Bisa SCD jika campaign sering berubah |
| `payment_method_type` | `dim_payment_method_type` | 1 row = 1 method type | Lookup kecil |
| `role`, `user_role` | `bridge_user_role` | depends on analysis need | Opsional untuk security/role analytics |

---

# 9. Rekomendasi desain BigQuery

## 9.1 Raw layer

Raw layer menyimpan data hasil ingest dengan perubahan minimal. Tambahkan metadata pipeline:

```text
_source_table
_source_pk
_op
_source_updated_at
_ingested_at
_batch_id
```

Rekomendasi BigQuery:

| Tabel raw | Partition field | Partition type | Cluster field |
|---|---|---|---|
| `raw_ride` | `_ingested_at` | DAY | `ride_id` |
| `raw_ride_status_history` | `_ingested_at` | DAY | `ride_id`, `ride_status_history_id` |
| `raw_payment_transaction` | `_ingested_at` | DAY | `ride_id`, `transaction_id` |
| `raw_ride_tracking_point` | `_ingested_at` | DAY | `ride_id`, `driver_id` |
| `raw_lookup_*` | Tidak perlu | Tidak perlu | Tidak perlu |

Raw layer lebih aman memakai `_ingested_at` karena tujuan utamanya audit, replay, dan debugging pipeline.

## 9.2 Staging/Clean layer

Staging layer membersihkan tipe data, dedup, casting, dan rename kolom. Untuk CDC, staging biasanya menyimpan latest record per primary key atau event history tergantung tipe tabel.

Contoh latest state untuk `ride`:

```sql
CREATE OR REPLACE TABLE clean.ride_latest AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY ride_id
      ORDER BY updated_at DESC, _ingested_at DESC
    ) AS row_num
  FROM raw.raw_ride r
)
WHERE row_num = 1;
```

## 9.3 Mart/Star layer

Mart layer memakai business event time untuk partition.

| Mart table | Partition field | Partition type | Cluster field | Alasan |
|---|---|---|---|---|
| `fact_ride` | `requested_at` | DAY | `driver_id`, `rider_id`, `ride_status`, `service_type` | Query ride biasanya berbasis tanggal request dan filter driver/status |
| `fact_ride_status_event` | `changed_at` | DAY | `ride_id`, `new_status` | Funnel lifecycle dan SLA |
| `fact_ride_location_event` | `captured_at` | DAY | `ride_id`, `location_type` | Analisis pickup/dropoff |
| `fact_ride_tracking_point` | `recorded_at` | DAY | `ride_id`, `driver_id` | GPS point besar, paling sering dibaca per ride dan waktu |
| `fact_ride_route_summary` | `completed_at` | DAY | `driver_id`, `service_type`, `city_code` | Dashboard ringkasan route |
| `fact_ride_fare` | `calculated_at` | DAY | `ride_id`, `fare_type` | Revenue dan audit fare |
| `fact_fare_component` | `created_at` | DAY | `fare_id`, `component_code` | Breakdown revenue |
| `fact_payment_transaction` | `created_at` atau `paid_at` | DAY | `ride_id`, `payment_status`, `provider_name` | Payment dashboard dan reconciliation |
| `fact_promo_usage` | `used_at` | DAY | `promotion_id`, `rider_id` | Campaign analytics |
| `fact_review` | `created_at` | DAY | `ride_id`, `reviewer_id`, `review_type` | Rating dan service quality |
| `fact_refund` | `requested_at` | DAY | `transaction_id`, `refund_status` | Net revenue, jika refund aktif |

Gunakan `require_partition_filter = TRUE` untuk tabel fact besar agar query tidak membaca semua partisi tanpa sengaja.

---

# 10. Contoh DDL BigQuery untuk mart utama

## 10.1 `fact_ride`

```sql
CREATE TABLE dwh.fact_ride (
  ride_id INT64,
  rider_id INT64,
  driver_id INT64,
  vehicle_id INT64,
  ride_status STRING,
  service_type STRING,
  city_code STRING,
  requested_at TIMESTAMP,
  accepted_at TIMESTAMP,
  arrived_at TIMESTAMP,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  cancelled_at TIMESTAMP,
  estimated_distance_km NUMERIC,
  estimated_duration_min NUMERIC,
  _loaded_at TIMESTAMP
)
PARTITION BY DATE(requested_at)
CLUSTER BY driver_id, rider_id, ride_status, service_type
OPTIONS (
  require_partition_filter = TRUE
);
```

## 10.2 `fact_ride_status_event`

```sql
CREATE TABLE dwh.fact_ride_status_event (
  ride_status_history_id INT64,
  ride_id INT64,
  old_status STRING,
  new_status STRING,
  changed_by_user_id INT64,
  reason_code STRING,
  reason_note STRING,
  changed_at TIMESTAMP,
  created_at TIMESTAMP,
  _loaded_at TIMESTAMP
)
PARTITION BY DATE(changed_at)
CLUSTER BY ride_id, new_status
OPTIONS (
  require_partition_filter = TRUE
);
```

## 10.3 `fact_ride_tracking_point`

```sql
CREATE TABLE dwh.fact_ride_tracking_point (
  tracking_point_id INT64,
  ride_id INT64,
  driver_id INT64,
  latitude NUMERIC,
  longitude NUMERIC,
  speed_kmh NUMERIC,
  heading_degree NUMERIC,
  accuracy_meter NUMERIC,
  recorded_at TIMESTAMP,
  created_at TIMESTAMP,
  _loaded_at TIMESTAMP
)
PARTITION BY DATE(recorded_at)
CLUSTER BY ride_id, driver_id
OPTIONS (
  require_partition_filter = TRUE,
  partition_expiration_days = 365
);
```

## 10.4 `fact_payment_transaction`

```sql
CREATE TABLE dwh.fact_payment_transaction (
  transaction_id INT64,
  ride_id INT64,
  user_payment_method_id INT64,
  provider_name STRING,
  provider_transaction_id STRING,
  idempotency_key STRING,
  amount NUMERIC,
  method_fee NUMERIC,
  currency_code STRING,
  payment_status STRING,
  failure_code STRING,
  failure_message STRING,
  authorized_at TIMESTAMP,
  captured_at TIMESTAMP,
  paid_at TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY ride_id, payment_status, provider_name
OPTIONS (
  require_partition_filter = TRUE
);
```

---

# 11. Pola ELT Postgres ke BigQuery via Trino

## 11.1 Batch full refresh lookup

```sql
CREATE OR REPLACE TABLE bigquery.raw.role AS
SELECT *
FROM postgresql.public.role;
```

## 11.2 Batch incremental master data

```sql
INSERT INTO bigquery.raw.vehicle
SELECT
  *,
  current_timestamp AS _ingested_at,
  'vehicle' AS _source_table
FROM postgresql.public.vehicle
WHERE updated_at >= current_timestamp - INTERVAL '1' DAY;
```

## 11.3 CDC raw event pattern

Jika menggunakan Debezium atau logical replication, simpan event mentah terlebih dahulu:

```text
raw_cdc.<table_name>
  key
  before
  after
  op
  source_lsn
  source_ts_ms
  ingested_at
```

Lalu transform ke clean layer:

```text
raw_cdc.ride
  -> clean.ride_latest
  -> dwh.fact_ride

raw_cdc.ride_status_history
  -> clean.ride_status_history_event
  -> dwh.fact_ride_status_event
```

---

# 12. Pipeline schedule yang disarankan

| Pipeline | Mode | Frequency lab | Frequency produksi | Isi |
|---|---|---:|---:|---|
| Reference batch | Batch | Harian/manual | Harian/deploy based | `role`, `payment_method_type` |
| Master batch | Batch incremental | 15 sampai 60 menit | 5 sampai 15 menit atau CDC | `user_account`, `vehicle`, `promotion`, `driver_document` |
| Driver status ingest | CDC/batch pendek | 1 sampai 5 menit | CDC | `driver_profile` |
| Ride lifecycle ingest | CDC | Real-time/simulasi | Real-time | `ride`, `ride_status_history` |
| Fare ingest | CDC | Real-time/simulasi | Real-time | `ride_fare`, `ride_fare_component` |
| Payment ingest | CDC | Real-time/simulasi | Real-time | `payment_transaction` |
| Tracking ingest | Micro-batch/CDC | 1 sampai 5 menit | Streaming/CDC/micro-batch | `ride_tracking_point` |
| Review ingest | Micro-batch/CDC | 5 sampai 15 menit | CDC/micro-batch | `review` |
| Promo usage ingest | CDC/micro-batch | 5 menit | CDC | `promo_usage` |
| Refund ingest | Opsional | Tidak aktif | CDC jika refund aktif | `payment_refund` |
| Mart build | Incremental SQL/dbt | 5 sampai 15 menit | 5 sampai 15 menit | star schema facts and dimensions |

---

# 13. Praktik aman untuk production-ready data warehouse

## 13.1 Jangan transform langsung dari OLTP ke mart

Gunakan minimal tiga layer:

```text
raw
clean/integration
mart/star
```

Raw menyimpan jejak sumber. Clean menormalisasi tipe data dan dedup. Mart menyajikan star schema yang stabil untuk analitik.

## 13.2 Tentukan grain sebelum transform

Contoh grain aman:

```text
fact_ride: 1 row = 1 ride
fact_ride_status_event: 1 row = 1 status change event
fact_ride_location_event: 1 row = 1 ride location event
fact_ride_tracking_point: 1 row = 1 GPS point
fact_ride_route_summary: 1 row = 1 ride route summary
fact_payment_transaction: 1 row = 1 payment attempt
fact_promo_usage: 1 row = 1 promo usage
fact_review: 1 row = 1 review
fact_refund: 1 row = 1 refund, jika refund aktif
```

## 13.3 Gunakan event time untuk fact

Untuk fact, prioritaskan timestamp bisnis:

```text
requested_at
changed_at
captured_at
recorded_at
calculated_at
paid_at
used_at
created_at
```

`_ingested_at` tetap penting, tetapi lebih cocok untuk audit pipeline dan partition pada raw layer.

## 13.4 Jangan jadikan `updated_at` sebagai CDC palsu untuk semua tabel

Incremental batch berbasis `updated_at` cukup untuk MVP, tetapi bisa bermasalah jika:

- ada update yang tidak mengubah `updated_at`;
- ada delete fisik;
- ada banyak update dalam window yang sama;
- kamu membutuhkan urutan perubahan status;
- ada late-arriving event.

Untuk `ride`, `ride_status_history`, `payment_transaction`, dan `ride_fare`, CDC lebih aman.

## 13.5 Pisahkan PII dari mart umum

Kolom berikut sebaiknya tidak masuk dashboard umum:

```text
email
phone_number
password_hash
provider_payment_token
document_file_url
license_number
document_number
```

Gunakan masking, hashing, atau exclude dari mart. Untuk analitik, biasanya cukup membawa surrogate key, status, flag verifikasi, tanggal pembuatan, dan informasi segmentasi non-sensitif.

## 13.6 Buat mart ringkasan untuk GPS

`ride_tracking_point` bisa sangat besar. Simpan detailnya untuk analisis teknis, tetapi buat mart ringkasan seperti:

```text
fact_ride_route_summary
```

Contoh kolom ringkasan:

```text
ride_id
driver_id
point_count
first_recorded_at
last_recorded_at
avg_speed_kmh
max_speed_kmh
avg_accuracy_meter
```

---

# 14. Keputusan praktis untuk project saat ini

Untuk project saat ini, urutan implementasi yang paling aman adalah:

Tahap 1, bangun batch dan mart dasar:

```text
Batch:
- role
- payment_method_type
- promotion
- user_account
- user_role
- driver_document
- vehicle
- driver_vehicle_assignment
- user_payment_method
```

Tahap 2, aktifkan CDC untuk core ride:

```text
CDC wajib:
- ride
- ride_status_history
- ride_fare
- ride_fare_component
- payment_transaction
```

Tahap 3, tambahkan event volume besar dan event opsional:

```text
CDC atau micro-batch:
- ride_location
- ride_tracking_point
- review
- promo_usage
```

Tahap 4, siapkan produksi tambahan:

```text
Opsional produksi:
- payment_refund
```

Dengan urutan ini, pipeline tidak langsung terlalu berat, tetapi tetap menunjukkan praktik enterprise: source dipisahkan dari mart, event lifecycle ditangkap dengan CDC, data besar dikendalikan lewat micro-batch atau partitioned fact, dan data sensitif tidak langsung masuk dashboard umum.

---

# 15. Rujukan teknis

- Debezium PostgreSQL connector: menghasilkan change event untuk operasi row-level `INSERT`, `UPDATE`, dan `DELETE`.
- PostgreSQL logical replication: mereplikasi perubahan data berdasarkan replication identity, biasanya primary key.
- BigQuery partitioned table: partition pruning membantu mengurangi data yang dibaca query.
- BigQuery require partition filter: membantu mengurangi full scan pada tabel besar.
- Trino BigQuery connector: dapat membaca data BigQuery dan digunakan dalam arsitektur federated query.
