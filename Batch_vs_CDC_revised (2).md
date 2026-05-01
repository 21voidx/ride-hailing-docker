# Ride-Hailing Realtime CRUD Generator: Batch vs CDC untuk ELT Postgres ke BigQuery

Dokumen ini disesuaikan dengan source PostgreSQL terbaru pada `init_postgres(3).sql` dan perilaku generator terbaru pada `generator(3).py`. Fokus revisi ini adalah memisahkan tabel source PostgreSQL ke tiga kelompok ingest untuk pipeline Postgres -> BigQuery:

1. **Batch append-only**: tabel event/log/detail yang hanya bertambah baris baru.
2. **Batch upsert**: tabel master/dimensi atau transaksi yang bisa berubah, tetapi masih aman diproses periodik dengan `MERGE`.
3. **Harus CDC**: tabel mutable yang perubahan statusnya cepat, berulang, dan penting ditangkap secara row-level.

Arsitektur yang diasumsikan:

```text
PostgreSQL OLTP
  -> batch extractor / CDC connector
  -> BigQuery raw/staging
  -> BigQuery clean/integration
  -> BigQuery mart/star schema
```

Catatan penting: jangan menyamakan append-only dengan tidak incremental. Append-only tetap sebaiknya incremental, tetapi bentuknya `INSERT` baris baru saja, bukan `MERGE`.

---

## 1. Prinsip keputusan

### 1.1 Batch append-only

Batch append-only cocok untuk tabel yang secara bisnis bersifat event, audit log, atau detail historis yang tidak perlu diubah setelah dibuat. Pola ini cocok untuk tabel besar karena lebih murah dan sederhana daripada `MERGE`.

Ciri tabel:

- hanya `INSERT` baris baru;
- tidak memiliki `updated_at` atau `deleted_at`;
- grain-nya event/detail historis;
- jika data terlambat beberapa menit, analitik tidak rusak;
- cocok memakai watermark berupa primary key naik atau event timestamp.

### 1.2 Batch upsert

Batch upsert cocok untuk tabel yang bisa berubah, tetapi tidak wajib real-time. Data dari Postgres bisa dibaca periodik lalu dimasukkan ke BigQuery dengan `MERGE` berdasarkan primary key.

Ciri tabel:

- memiliki `updated_at`, status, flag aktif, atau soft delete;
- perubahan tidak perlu ditangkap per detik;
- dashboard masih aman jika refresh setiap 5-60 menit;
- cocok untuk master data, dimensi, dan transaksi yang tidak berubah terlalu cepat.

### 1.3 Harus CDC

CDC dipakai untuk tabel yang mengalami `INSERT`, `UPDATE`, atau `DELETE` penting dan cepat. BigQuery CDC memang dirancang untuk menerapkan operasi `upsert` dan `delete` secara streaming, sedangkan Debezium PostgreSQL menghasilkan change event untuk setiap operasi row-level `INSERT`, `UPDATE`, dan `DELETE`.

Ciri tabel:

- perubahan status terjadi berulang dalam satu lifecycle;
- urutan perubahan penting;
- batch polling bisa hanya menangkap status akhir;
- dipakai untuk dashboard operasional, payment, dan SLA.

---

## 2. Ringkasan final nama tabel per kategori

### 2.1 Batch append-only

Tabel berikut bisa diproses dengan batch append-only:

```text
ride_status_history
ride_location
ride_tracking_point
ride_fare
ride_fare_component
promo_usage
```

Alasan umum: tabel-tabel ini berisi event/detail historis dan pada schema tidak memiliki `updated_at` atau `deleted_at`. Di generator, tabel ini juga diisi dengan pola `INSERT`, misalnya status history, lokasi ride, tracking point, fare final, fare component, dan promo usage.

### 2.2 Batch upsert

Tabel berikut bisa diproses dengan batch upsert:

```text
role
payment_method_type
promotion
user_account
user_role
driver_document
vehicle
driver_vehicle_assignment
user_payment_method
review
payment_refund
```

Catatan:

- `role` dan `payment_method_type` sebenarnya lookup kecil. Untuk project sederhana, keduanya boleh full refresh. Namun karena kategori yang diminta hanya append-only, upsert, dan CDC, keduanya dimasukkan ke batch upsert/snapshot kecil.
- `payment_refund` belum aktif di generator saat ini, tetapi schema sudah tersedia. Jika refund mulai aktif dan butuh status cepat, pindahkan ke CDC.
- `review` bisa batch upsert karena ada `updated_at` dan `deleted_at`; review tidak aman dianggap append-only murni.

### 2.3 Harus CDC

Tabel berikut sebaiknya masuk CDC:

```text
ride
driver_profile
payment_transaction
```

Alasan umum:

- `ride` mengalami banyak update status dalam satu lifecycle: `REQUESTED`, `ACCEPTED`, `ARRIVED`, `IN_PROGRESS`, lalu `COMPLETED`, `CANCELLED`, atau `PAYMENT_FAILED`.
- `driver_profile` berubah cepat karena `driver_status` berpindah antara `AVAILABLE` dan `ON_RIDE`.
- `payment_transaction` menentukan status finansial. Walaupun generator saat ini hanya insert status final `PAID` atau `FAILED`, schema mendukung status seperti `AUTHORIZED`, `CAPTURED`, `PAID`, `FAILED`, dan `REFUNDED`. Di sistem nyata, ini lebih aman ditangkap CDC.

---

## 3. Tabel keputusan per source table

| Tabel PostgreSQL | Kategori ingest | Key / watermark | Alasan keputusan |
|---|---|---|---|
| `ride_status_history` | Batch append-only | `ride_status_history_id` atau `changed_at` | Audit log status ride. Setiap perubahan status dibuat sebagai baris baru. |
| `ride_location` | Batch append-only | `ride_location_id` atau `captured_at` | Event lokasi pickup/dropoff requested dan actual. Tidak ada `updated_at`. |
| `ride_tracking_point` | Batch append-only | `tracking_point_id` atau `recorded_at` | GPS point besar dan insert-only. Lebih efisien micro-batch append. |
| `ride_fare` | Batch append-only | `fare_id` atau `calculated_at` | Fare dibuat sebagai versi/final calculation. Tidak ada `updated_at`. |
| `ride_fare_component` | Batch append-only | `fare_component_id` atau `created_at` | Detail komponen fare dari `ride_fare`. Tidak perlu update. |
| `promo_usage` | Batch append-only | `promo_usage_id` atau `used_at` | Log pemakaian promo. Ada `UNIQUE (ride_id)`, tetapi tidak ada `updated_at`. |
| `role` | Batch upsert / snapshot kecil | `role_id`, `updated_at` | Lookup kecil. Bisa full refresh, tetapi aman juga upsert. |
| `payment_method_type` | Batch upsert / snapshot kecil | `payment_method_type_id`, `updated_at` | Lookup kecil. Bisa full refresh, tetapi aman juga upsert. |
| `promotion` | Batch upsert | `promotion_id`, `updated_at` | Promo bisa berubah status, periode aktif, limit, atau nilai diskon. |
| `user_account` | Batch upsert | `user_id`, `updated_at` | Akun bisa berubah status, verifikasi, login terakhir, atau soft delete. |
| `user_role` | Batch upsert | `user_id`, `role_id` | Relasi role bisa aktif/nonaktif. Tidak ada `updated_at`, jadi bisa snapshot periodik. |
| `driver_document` | Batch upsert | `document_id`, `updated_at` | Status verifikasi dokumen bisa berubah. |
| `vehicle` | Batch upsert | `vehicle_id`, `updated_at` | Status kendaraan dan soft delete bisa berubah. |
| `driver_vehicle_assignment` | Batch upsert | `assignment_id`, `updated_at` | Assignment bisa ditutup lewat `assigned_to` dan `is_active`. |
| `user_payment_method` | Batch upsert | `user_payment_method_id`, `updated_at` | Status metode bayar bisa berubah atau soft delete. |
| `review` | Batch upsert | `review_id`, `updated_at` | Ada `updated_at` dan `deleted_at`, berarti bisa diedit/dihapus. |
| `payment_refund` | Batch upsert, naik ke CDC jika aktif | `refund_id`, `updated_at` | Refund punya status `REQUESTED`, `COMPLETED`, `FAILED`; saat ini belum diisi generator. |
| `ride` | Harus CDC | `ride_id`, source LSN/event time | Core lifecycle ride, banyak update status dan timestamp. |
| `driver_profile` | Harus CDC | `driver_id`, source LSN/event time | Status driver berubah cepat saat menerima/menyelesaikan ride. |
| `payment_transaction` | Harus CDC | `transaction_id`, source LSN/event time | Status pembayaran penting untuk revenue dan reconciliation. |

---

## 4. Kenapa klasifikasi lama perlu dikoreksi

Dokumen lama terlalu banyak menempatkan tabel event ke CDC. Itu kurang efisien untuk pipeline yang juga punya batch. Tidak semua tabel event harus CDC. Jika tabel benar-benar insert-only dan tidak perlu real-time detik-ke-detik, batch append-only lebih sederhana, murah, dan cukup akurat.

Koreksi utama:

| Tabel | Keputusan lama yang kurang tepat | Keputusan baru |
|---|---|---|
| `ride_status_history` | CDC wajib | Batch append-only, CDC opsional jika dashboard SLA harus real-time |
| `ride_location` | CDC/micro-batch | Batch append-only |
| `ride_tracking_point` | CDC/streaming atau micro-batch | Batch append-only/micro-batch append |
| `ride_fare` | CDC wajib | Batch append-only, karena tidak ada `updated_at` dan ada `fare_version` |
| `ride_fare_component` | CDC | Batch append-only |
| `promo_usage` | CDC/micro-batch | Batch append-only |
| `review` | CDC/micro-batch | Batch upsert, karena bisa diedit/dihapus |
| `payment_refund` | CDC jika aktif | Batch upsert dulu; CDC jika refund sudah aktif dan SLA finansial menuntut real-time |

---

## 5. Pola implementasi batch append-only

Gunakan primary key naik atau event timestamp sebagai watermark. Primary key lebih aman untuk menghindari masalah timestamp sama persis.

Contoh `ride_tracking_point`:

```sql
INSERT INTO bigquery.raw.ride_tracking_point
SELECT
  *,
  current_timestamp AS _ingested_at,
  'ride_tracking_point' AS _source_table
FROM postgresql.public.ride_tracking_point
WHERE tracking_point_id > (
  SELECT COALESCE(MAX(tracking_point_id), 0)
  FROM bigquery.raw.ride_tracking_point
);
```

Contoh `ride_status_history`:

```sql
INSERT INTO bigquery.raw.ride_status_history
SELECT
  *,
  current_timestamp AS _ingested_at,
  'ride_status_history' AS _source_table
FROM postgresql.public.ride_status_history
WHERE ride_status_history_id > (
  SELECT COALESCE(MAX(ride_status_history_id), 0)
  FROM bigquery.raw.ride_status_history
);
```

---

## 6. Pola implementasi batch upsert

Batch upsert cocok untuk tabel yang punya `updated_at`. Muat data berubah ke staging, lalu `MERGE` ke tabel target BigQuery.

Contoh `vehicle`:

```sql
MERGE bigquery.raw.vehicle T
USING bigquery.stage.vehicle_delta S
ON T.vehicle_id = S.vehicle_id
WHEN MATCHED THEN UPDATE SET
  license_plate = S.license_plate,
  vehicle_make = S.vehicle_make,
  vehicle_model = S.vehicle_model,
  vehicle_status = S.vehicle_status,
  updated_at = S.updated_at,
  deleted_at = S.deleted_at,
  _ingested_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT ROW;
```

Contoh ekstraksi delta dari Postgres:

```sql
SELECT *
FROM postgresql.public.vehicle
WHERE updated_at >= TIMESTAMP '2026-05-01 00:00:00';
```

Untuk `user_role` yang tidak punya `updated_at`, pilih salah satu:

1. full snapshot periodik ke staging lalu rekonsiliasi;
2. tambahkan kolom `updated_at` pada source;
3. masukkan ke CDC jika perubahan role harus dilacak cepat.

---

## 7. Pola implementasi CDC

CDC wajib dipakai untuk:

```text
ride
driver_profile
payment_transaction
```

Simpan event CDC mentah di raw terlebih dahulu:

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

Lalu buat latest state di clean layer.

Contoh latest state `ride`:

```sql
CREATE OR REPLACE TABLE clean.ride_latest AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY ride_id
      ORDER BY source_ts_ms DESC, ingested_at DESC
    ) AS row_num
  FROM raw_cdc.ride r
  WHERE op != 'd'
)
WHERE row_num = 1;
```

Untuk audit perubahan status ride, jangan hanya mengandalkan `ride_latest`. Tetap pakai `ride_status_history` sebagai fact event karena tabel itu menyimpan detail urutan status secara eksplisit.

---

## 8. Schedule pipeline yang disarankan

| Pipeline | Mode | Frekuensi lab | Frekuensi produksi | Tabel |
|---|---|---:|---:|---|
| Event append batch | Batch append-only | 1-5 menit | 1-5 menit atau lebih cepat sesuai SLA | `ride_status_history`, `ride_location`, `ride_tracking_point`, `ride_fare`, `ride_fare_component`, `promo_usage` |
| Master/dim batch | Batch upsert | 15-60 menit | 5-15 menit | `promotion`, `user_account`, `user_role`, `driver_document`, `vehicle`, `driver_vehicle_assignment`, `user_payment_method`, `review`, `payment_refund` |
| Lookup snapshot | Batch upsert/snapshot kecil | Harian/manual | Harian/deploy based | `role`, `payment_method_type` |
| Core ride CDC | CDC | Real-time/simulasi | Real-time | `ride` |
| Driver status CDC | CDC | Real-time/simulasi | Real-time | `driver_profile` |
| Payment CDC | CDC | Real-time/simulasi | Real-time | `payment_transaction` |

---

## 9. Mapping source ke target star schema

| Source table | Ingest mode | Target star schema | Grain target |
|---|---|---|---|
| `ride` | CDC | `fact_ride`, `fact_ride_current` | 1 row = 1 ride |
| `ride_status_history` | Batch append-only | `fact_ride_status_event` | 1 row = 1 status change |
| `ride_location` | Batch append-only | `fact_ride_location_event` | 1 row = 1 location event per ride |
| `ride_tracking_point` | Batch append-only | `fact_ride_tracking_point`, `fact_ride_route_summary` | 1 row = 1 GPS point / 1 summary per ride |
| `ride_fare` | Batch append-only | `fact_ride_fare`, `fact_revenue` | 1 row = 1 fare version per ride |
| `ride_fare_component` | Batch append-only | `fact_fare_component` | 1 row = 1 fare component |
| `promo_usage` | Batch append-only | `fact_promo_usage` | 1 row = 1 promo usage |
| `payment_transaction` | CDC | `fact_payment_transaction` | 1 row = 1 payment attempt/current state |
| `payment_refund` | Batch upsert | `fact_refund` | 1 row = 1 refund current state |
| `review` | Batch upsert | `fact_review` | 1 row = 1 review current state |
| `user_account` | Batch upsert | `dim_user`, `dim_rider` | 1 row = 1 user current/version |
| `driver_profile` | CDC | `dim_driver_current`, `fact_driver_status_snapshot` | 1 row = 1 driver current/status event |
| `vehicle` | Batch upsert | `dim_vehicle` | 1 row = 1 vehicle current/version |
| `driver_vehicle_assignment` | Batch upsert | `bridge_driver_vehicle_assignment` | 1 row = 1 assignment period |
| `driver_document` | Batch upsert | `dim_driver_document` | 1 row = 1 document current state |
| `promotion` | Batch upsert | `dim_promotion` | 1 row = 1 promotion current/version |
| `user_payment_method` | Batch upsert | `dim_user_payment_method_masked` | 1 row = 1 payment method current state |
| `role` | Batch upsert/snapshot | `dim_role` | 1 row = 1 role |
| `payment_method_type` | Batch upsert/snapshot | `dim_payment_method_type` | 1 row = 1 payment method type |
| `user_role` | Batch upsert/snapshot | `bridge_user_role` | 1 row = 1 user-role relation |

---

## 10. Rekomendasi desain BigQuery

### 10.1 Raw layer

Tambahkan metadata pipeline untuk semua tabel raw:

```text
_source_table
_source_pk
_op
_source_updated_at
_ingested_at
_batch_id
```

Untuk CDC raw, tambahkan:

```text
source_lsn
source_ts_ms
before
after
op
```

### 10.2 Partition dan cluster

| Tabel mart | Partition field | Cluster field |
|---|---|---|
| `fact_ride` | `requested_at` | `driver_id`, `rider_id`, `ride_status`, `service_type` |
| `fact_ride_status_event` | `changed_at` | `ride_id`, `new_status` |
| `fact_ride_location_event` | `captured_at` | `ride_id`, `location_type` |
| `fact_ride_tracking_point` | `recorded_at` | `ride_id`, `driver_id` |
| `fact_ride_fare` | `calculated_at` | `ride_id`, `fare_type` |
| `fact_fare_component` | `created_at` | `fare_id`, `component_code` |
| `fact_payment_transaction` | `created_at` atau `paid_at` | `ride_id`, `payment_status`, `provider_name` |
| `fact_promo_usage` | `used_at` | `promotion_id`, `rider_id` |
| `fact_review` | `created_at` | `ride_id`, `reviewer_id`, `review_type` |
| `fact_refund` | `requested_at` | `transaction_id`, `refund_status` |

Gunakan `require_partition_filter = TRUE` untuk fact besar agar query tidak membaca semua partisi tanpa sengaja.

---

## 11. Keputusan praktis untuk project saat ini

Tahap implementasi yang paling masuk akal:

### Tahap 1: Batch append-only

```text
ride_status_history
ride_location
ride_tracking_point
ride_fare
ride_fare_component
promo_usage
```

Tahap ini menunjukkan kemampuan batch incremental tanpa kompleksitas `MERGE`.

### Tahap 2: Batch upsert

```text
role
payment_method_type
promotion
user_account
user_role
driver_document
vehicle
driver_vehicle_assignment
user_payment_method
review
payment_refund
```

Tahap ini menunjukkan kemampuan `MERGE` untuk tabel mutable dan lookup/master.

### Tahap 3: CDC wajib

```text
ride
driver_profile
payment_transaction
```

Tahap ini menunjukkan kemampuan pipeline menangkap perubahan row-level penting dari PostgreSQL ke BigQuery.

---

## 12. Catatan produksi

1. Jangan CDC semua tabel hanya karena tool mendukung CDC. Itu boros dan membuat pipeline sulit dipelihara.
2. Jangan batch append untuk tabel yang memiliki perubahan status penting seperti `ride`, `driver_profile`, dan `payment_transaction`.
3. Jangan anggap `review` append-only karena schema memiliki `updated_at` dan `deleted_at`.
4. Jangan anggap `payment_refund` append-only karena status refund bisa berubah.
5. Untuk `ride_tracking_point`, simpan detail GPS di raw/staging, lalu buat mart ringkasan seperti `fact_ride_route_summary` agar dashboard tidak membaca GPS point mentah terus-menerus.
6. Pisahkan PII dari mart umum, terutama `email`, `phone_number`, `password_hash`, `provider_payment_token`, `document_file_url`, `license_number`, dan `document_number`.

---

## 13. Rujukan teknis

- BigQuery CDC: https://docs.cloud.google.com/bigquery/docs/change-data-capture
- dbt incremental models: https://docs.getdbt.com/docs/build/incremental-models
- dbt incremental strategy: https://docs.getdbt.com/docs/build/incremental-strategy
- Debezium PostgreSQL connector: https://debezium.io/documentation/reference/3.4/connectors/postgresql.html
