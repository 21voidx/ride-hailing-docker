# Ride-Hailing Realtime CRUD Generator + CDC ELT ke BigQuery

Project ini berisi PostgreSQL OLTP ride-hailing, generator CRUD realtime ringan, dan rancangan teknis CDC untuk ELT menuju star schema.

Stack dasar lokal:

- PostgreSQL 16
- Python realtime CRUD generator
- Adminer UI

Stack CDC yang direkomendasikan:

- PostgreSQL source
- Debezium PostgreSQL connector
- Kafka
- Schema Registry
- Kafka Connect
- GCS Sink Connector
- Google Cloud Storage sebagai landing zone
- Airflow untuk load Parquet hourly ke BigQuery
- BigQuery staging, ODS/current table, dan star schema mart

## Tujuan pipeline

Target pipeline:

```text
PostgreSQL OLTP
  -> Debezium CDC
  -> Kafka topics, value format Avro
  -> GCS Sink Connector
  -> GCS files, format Parquet, flush setiap 1 jam
  -> Airflow hourly DAG
  -> BigQuery raw/staging CDC tables
  -> BigQuery MERGE ke ODS/current tables
  -> BigQuery star schema: fact dan dimension
```

Prinsip penting:

1. Kafka menyimpan event CDC, bukan tabel final.
2. GCS menyimpan immutable lake files dalam format Parquet.
3. BigQuery staging menerima append-only CDC rows.
4. BigQuery ODS/current melakukan dedup dan merge berdasarkan primary key dan urutan event.
5. Star schema dibangun dari ODS/current atau dari refined CDC, bukan langsung dari file Kafka.

Jangan langsung load Parquet CDC ke `fact_ride` atau `dim_driver`. CDC masih berisi insert, update, delete, metadata, dan beberapa versi row. Star schema butuh proses ELT setelah data masuk BigQuery.

---

## Kenapa Kafka Avro dan GCS Parquet?

Kafka memakai Avro karena Debezium dan Kafka Connect butuh schema agar evolusi kolom lebih terkontrol. AvroConverter di Kafka Connect terintegrasi dengan Schema Registry dan otomatis membawa schema dari connector ke Kafka record.

GCS memakai Parquet karena BigQuery bisa load Parquet dari Cloud Storage, Parquet bersifat columnar, dan cocok untuk batch hourly analytic ingestion.

Catatan teknis:

- Avro adalah format record di Kafka.
- Parquet adalah format file di GCS.
- Schema Registry tetap penting karena GCS Sink Parquet membutuhkan Kafka record yang berschema.
- Setiap topik sebaiknya mewakili satu source table.

---

## Tabel CDC dari OLTP ride-hailing

Berdasarkan ERD project ini, tabel-tabel utama CDC adalah:

| Source table | CDC? | Alasan |
|---|---:|---|
| `user_account` | Ya, hybrid | SCD/user status, soft delete, verified timestamp |
| `user_role` | Ya | Role user berubah dan berdampak ke akses historis |
| `driver_profile` | Ya | Status, verification, rating cache, suspension |
| `driver_document` | Ya | Dokumen diverifikasi, ditolak, expired |
| `vehicle` | Ya | Vehicle status, verification, soft delete |
| `driver_vehicle_assignment` | Ya | Assignment punya rentang waktu dan historis penting |
| `ride` | Ya, wajib | Core transaction lifecycle, banyak update status |
| `ride_status_history` | Ya, wajib | Event timeline dan audit lifecycle |
| `ride_location` | Ya | Pickup/dropoff requested dan actual |
| `ride_tracking_point` | Ya atau micro-batch | Volume tinggi, append-only GPS points |
| `ride_fare` | Ya | Fare bisa estimated, final, adjusted |
| `ride_fare_component` | Ya | Breakdown biaya |
| `user_payment_method` | Ya, hati-hati PII/token | Status metode bayar bisa berubah |
| `payment_transaction` | Ya, wajib | Payment retry, paid, failed, captured |
| `payment_refund` | Ya | Refund lifecycle |
| `review` | Ya | Review bisa insert, update, soft delete |
| `promotion` | Batch atau CDC | Biasanya lookup/campaign, tetapi bisa berubah saat aktif |
| `promo_usage` | Ya, wajib | Pemakaian promo harus auditable |
| `role` | Batch | Lookup kecil dan jarang berubah |
| `payment_method_type` | Batch | Lookup kecil dan jarang berubah |

Untuk star schema, tabel seperti `ride`, `ride_fare`, `payment_transaction`, `promo_usage`, dan `review` akan menjadi sumber fact. Tabel seperti `user_account`, `driver_profile`, `vehicle`, `promotion`, dan `payment_method_type` akan menjadi sumber dimension.

---

## Konsep topic naming

Gunakan topic per table.

Contoh default Debezium:

```text
ride_oltp.public.user_account
ride_oltp.public.driver_profile
ride_oltp.public.vehicle
ride_oltp.public.ride
ride_oltp.public.ride_status_history
ride_oltp.public.ride_tracking_point
ride_oltp.public.ride_fare
ride_oltp.public.payment_transaction
ride_oltp.public.review
ride_oltp.public.promo_usage
```

Untuk memudahkan GCS dan Airflow, boleh route nama topic menjadi lebih pendek:

```text
cdc.public.user_account
cdc.public.ride
cdc.public.payment_transaction
```

Tetapi untuk awal, pakai default Debezium lebih aman.

---

## PostgreSQL source configuration

PostgreSQL harus mengaktifkan logical replication.

Di `postgresql.conf` atau command Docker:

```text
wal_level=logical
max_wal_senders=10
max_replication_slots=10
```

User Debezium perlu izin replication.

Contoh SQL:

```sql
CREATE ROLE debezium WITH LOGIN PASSWORD 'dbz_pass' REPLICATION;
GRANT CONNECT ON DATABASE ride_db TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
```

Pastikan setiap tabel CDC punya primary key. Ini penting untuk key Kafka dan proses MERGE BigQuery.

---

## Kafka dan Schema Registry configuration

Untuk Avro, Kafka Connect worker harus memakai AvroConverter.

Contoh `connect-distributed.properties` atau environment Docker Connect:

```properties
bootstrap.servers=kafka:9092
group.id=connect-cluster
config.storage.topic=_connect_configs
offset.storage.topic=_connect_offsets
status.storage.topic=_connect_status
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1

key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://schema-registry:8081
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081

key.converter.schemas.enable=true
value.converter.schemas.enable=true

offset.flush.interval.ms=60000
plugin.path=/usr/share/java,/usr/share/confluent-hub-components
```

Untuk production, replication factor jangan 1. Untuk lab lokal, 1 cukup.

---

## Debezium PostgreSQL connector dengan unwrap SMT

Kamu meminta Debezium transform unwrap. Gunakan `ExtractNewRecordState`.

Ada dua pola:

1. Apply unwrap di Debezium source connector.
2. Apply unwrap di sink connector.

Untuk pipeline ini, saya ikuti permintaanmu: unwrap di Debezium source connector. Namun, catatan penting: jika unwrap dilakukan di source, Kafka tidak lagi menyimpan envelope Debezium lengkap. Ini lebih simpel untuk GCS/BigQuery, tetapi metadata `before/after` penuh hilang. Untuk audit sangat ketat, pertimbangkan menyimpan satu raw topic envelope dan satu flattened topic.

### Connector JSON

Simpan sebagai `connectors/debezium-postgres-ride.json`:

```json
{
  "name": "debezium-postgres-ride",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "dbz_pass",
    "database.dbname": "ride_db",
    "topic.prefix": "ride_oltp",

    "plugin.name": "pgoutput",
    "slot.name": "ride_oltp_slot",
    "publication.name": "ride_oltp_publication",
    "publication.autocreate.mode": "filtered",

    "schema.include.list": "public",
    "table.include.list": "public.user_account,public.user_role,public.driver_profile,public.driver_document,public.vehicle,public.driver_vehicle_assignment,public.ride,public.ride_status_history,public.ride_location,public.ride_tracking_point,public.ride_fare,public.ride_fare_component,public.user_payment_method,public.payment_transaction,public.payment_refund,public.review,public.promotion,public.promo_usage,public.role,public.payment_method_type",

    "snapshot.mode": "initial",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "double",
    "time.precision.mode": "adaptive_time_microseconds",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table,source.ts_ms,source.db,source.schema,lsn",
    "transforms.unwrap.add.fields.prefix": "__"
  }
}
```

Register connector:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @connectors/debezium-postgres-ride.json
```

Cek status:

```bash
curl http://localhost:8083/connectors/debezium-postgres-ride/status | jq
```

### Kenapa `delete.tombstone.handling.mode = rewrite`?

Dengan unwrap default, delete sering hilang atau menjadi tombstone yang sulit diload ke Parquet. Untuk ELT ke BigQuery, lebih mudah jika delete menjadi row biasa dengan field:

```text
__deleted = true
__op = d
```

Dengan begitu, BigQuery bisa melakukan MERGE dan menandai row sebagai deleted.

### Metadata yang penting untuk BigQuery

Setelah unwrap, setiap record punya kolom tambahan seperti:

```text
__op
__table
__source_ts_ms
__source_db
__source_schema
__lsn
__deleted
```

Gunakan metadata ini untuk:

- dedup event
- urutan update
- audit source
- MERGE ke current table
- soft delete di ODS

---

## GCS Sink Connector: Kafka Avro ke GCS Parquet hourly

Gunakan Confluent GCS Sink Connector.

Target:

```text
Kafka topic Avro -> GCS object Parquet
```

### Struktur GCS yang direkomendasikan

Gunakan bucket layout:

```text
gs://my-ride-lake/landing/cdc/<topic>/year=YYYY/month=MM/day=DD/hour=HH/*.parquet
```

Contoh:

```text
gs://my-ride-lake/landing/cdc/ride_oltp.public.ride/year=2026/month=04/day=30/hour=10/*.parquet
```

Kenapa per topic dan per hour?

1. Airflow mudah load prefix hourly.
2. BigQuery load job bisa pakai wildcard per prefix.
3. File tidak terlalu kecil jika flush per jam.
4. Backfill bisa dilakukan per table dan per hour.
5. Star schema job bisa menunggu semua source table utama untuk jam tersebut.

---

## GCS Sink Connector config

Simpan sebagai `connectors/gcs-sink-parquet-hourly.json`:

```json
{
  "name": "gcs-sink-parquet-hourly",
  "config": {
    "connector.class": "io.confluent.connect.gcs.GcsSinkConnector",
    "tasks.max": "2",

    "topics.regex": "ride_oltp\\.public\\.(user_account|user_role|driver_profile|driver_document|vehicle|driver_vehicle_assignment|ride|ride_status_history|ride_location|ride_tracking_point|ride_fare|ride_fare_component|user_payment_method|payment_transaction|payment_refund|review|promotion|promo_usage|role|payment_method_type)",

    "gcs.bucket.name": "my-ride-lake",
    "gcs.credentials.path": "/etc/kafka-connect/secrets/gcp-service-account.json",

    "storage.class": "io.confluent.connect.gcs.storage.GcsStorage",
    "format.class": "io.confluent.connect.gcs.format.parquet.ParquetFormat",
    "schema.compatibility": "BACKWARD",

    "topics.dir": "landing/cdc",
    "directory.delim": "/",
    "file.delim": "-",

    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "partition.duration.ms": "3600000",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "locale": "en-US",
    "timezone": "UTC",
    "timestamp.extractor": "Wallclock",

    "flush.size": "50000",
    "rotate.schedule.interval.ms": "3600000",

    "behavior.on.null.values": "ignore",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

Register connector:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @connectors/gcs-sink-parquet-hourly.json
```

Cek status:

```bash
curl http://localhost:8083/connectors/gcs-sink-parquet-hourly/status | jq
```

---

## Penjelasan konfigurasi GCS Sink yang penting

### `format.class = ParquetFormat`

Ini membuat output file di GCS menjadi Parquet.

```json
"format.class": "io.confluent.connect.gcs.format.parquet.ParquetFormat"
```

### `rotate.schedule.interval.ms = 3600000`

Ini membuat connector commit file berdasarkan jadwal setiap 1 jam.

```json
"rotate.schedule.interval.ms": "3600000"
```

Gunakan `rotate.schedule.interval.ms`, bukan hanya `rotate.interval.ms`, jika kamu ingin file commit di batas jam yang rapi seperti 10:00, 11:00, 12:00.

### `flush.size = 50000`

Ini adalah batas jumlah record per file. File akan ditutup jika mencapai 50.000 record sebelum 1 jam.

```json
"flush.size": "50000"
```

Jadi file akan commit jika salah satu terpenuhi:

```text
record mencapai flush.size
atau jadwal hourly rotation tercapai
```

### `TimeBasedPartitioner`

Ini membuat folder berdasarkan waktu ingest connector.

```json
"partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner"
```

Dengan path:

```json
"path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH"
```

Maka GCS menjadi:

```text
landing/cdc/<topic>/year=2026/month=04/day=30/hour=10/
```

### `timestamp.extractor = Wallclock`

Untuk pipeline hourly ingestion, `Wallclock` paling aman. Folder GCS mengikuti waktu connector menerima event, bukan event time dari source.

Kalau kamu ingin folder berdasarkan waktu source event, kamu bisa memakai `RecordField`, tetapi kamu harus memastikan field timestamp yang dipakai bertipe timestamp Connect, bukan sekadar integer epoch. Untuk awal, jangan cari penyakit. Pakai `Wallclock`, lalu simpan event time di BigQuery melalui `__source_ts_ms`.

### `behavior.on.null.values = ignore`

Karena delete sudah diubah menjadi row biasa lewat unwrap rewrite, tombstone/null value tidak perlu masuk GCS.

---

## Banyak file Parquet di GCS: bagaimana cara load ke BigQuery?

Jangan load file satu per satu. Load per prefix hourly menggunakan wildcard.

Contoh prefix:

```text
gs://my-ride-lake/landing/cdc/ride_oltp.public.ride/year=2026/month=04/day=30/hour=10/*.parquet
```

BigQuery bisa load Parquet dari Cloud Storage dan append/overwrite ke table. Untuk wildcard URI, semua file yang cocok harus punya schema yang kompatibel.

Rekomendasi:

1. Satu BigQuery staging table per Kafka topic/source table.
2. Load hourly prefix ke staging append-only.
3. Tambahkan kolom load batch di Airflow setelah load, atau gunakan metadata table untuk mencatat prefix yang sudah diproses.
4. MERGE dari staging ke ODS/current table.
5. Build star schema dari ODS/current atau refined layer.

---

## Dataset BigQuery yang disarankan

```text
ride_raw_cdc      -- append-only hasil load Parquet dari GCS
ride_ods          -- current tables hasil MERGE CDC
ride_mart         -- star schema fact/dim
ride_audit        -- watermark, processed files, job logs
```

Contoh tables:

```text
ride_raw_cdc.raw_ride
ride_raw_cdc.raw_payment_transaction
ride_raw_cdc.raw_driver_profile

ride_ods.ride_current
ride_ods.payment_transaction_current
ride_ods.driver_profile_current

ride_mart.fact_ride
ride_mart.fact_payment
ride_mart.dim_driver
ride_mart.dim_rider
ride_mart.dim_vehicle
ride_mart.dim_promotion
```

---

## BigQuery staging table strategy

Untuk awal, izinkan BigQuery autodetect schema dari Parquet. Namun untuk production, lebih aman membuat schema eksplisit.

Masalah yang sering muncul:

1. Schema evolusi dari source table.
2. File Parquet dalam satu wildcard prefix punya schema berbeda.
3. Decimal/numeric berubah tipe.
4. Field delete `__deleted` hanya muncul ketika ada delete.
5. Column order dan nullable field berubah.

Rekomendasi production:

- Pakai staging table per topic.
- Pakai `schemaUpdateOptions: ALLOW_FIELD_ADDITION` jika schema berubah menambah kolom.
- Hindari drop/rename kolom langsung di source tanpa migrasi terencana.
- Gunakan explicit schema untuk raw table penting.
- Jadikan semua kolom raw nullable kecuali metadata teknis yang kamu kontrol.

---

## Airflow DAG: load hourly Parquet dari GCS ke BigQuery

DAG berjalan setiap jam dan memproses data hour sebelumnya.

Kenapa previous hour?

Karena GCS Sink flush hourly di batas jam. Airflow sebaiknya memberi buffer 10 sampai 15 menit agar file sudah tertutup dan visible di GCS.

Contoh schedule:

```text
Kafka Connect flush: setiap jam
Airflow DAG: menit ke-15 setiap jam
Load prefix: jam sebelumnya
```

Contoh:

```text
11:00 GCS Sink flush hour=10
11:15 Airflow load hour=10
```

---

## Contoh Airflow DAG

Simpan sebagai `dags/load_gcs_parquet_to_bigquery.py`:

```python
from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

PROJECT_ID = "my-gcp-project"
BUCKET = "my-ride-lake"
LOCATION = "asia-southeast2"
RAW_DATASET = "ride_raw_cdc"

TOPIC_TABLES = {
    "ride_oltp.public.user_account": "raw_user_account",
    "ride_oltp.public.user_role": "raw_user_role",
    "ride_oltp.public.driver_profile": "raw_driver_profile",
    "ride_oltp.public.driver_document": "raw_driver_document",
    "ride_oltp.public.vehicle": "raw_vehicle",
    "ride_oltp.public.driver_vehicle_assignment": "raw_driver_vehicle_assignment",
    "ride_oltp.public.ride": "raw_ride",
    "ride_oltp.public.ride_status_history": "raw_ride_status_history",
    "ride_oltp.public.ride_location": "raw_ride_location",
    "ride_oltp.public.ride_tracking_point": "raw_ride_tracking_point",
    "ride_oltp.public.ride_fare": "raw_ride_fare",
    "ride_oltp.public.ride_fare_component": "raw_ride_fare_component",
    "ride_oltp.public.user_payment_method": "raw_user_payment_method",
    "ride_oltp.public.payment_transaction": "raw_payment_transaction",
    "ride_oltp.public.payment_refund": "raw_payment_refund",
    "ride_oltp.public.review": "raw_review",
    "ride_oltp.public.promotion": "raw_promotion",
    "ride_oltp.public.promo_usage": "raw_promo_usage",
    "ride_oltp.public.role": "raw_role",
    "ride_oltp.public.payment_method_type": "raw_payment_method_type",
}


def hourly_prefix(topic: str) -> str:
    return (
        "landing/cdc/"
        f"{topic}/"
        "year={{ (data_interval_start - macros.timedelta(hours=1)).strftime('%Y') }}/"
        "month={{ (data_interval_start - macros.timedelta(hours=1)).strftime('%m') }}/"
        "day={{ (data_interval_start - macros.timedelta(hours=1)).strftime('%d') }}/"
        "hour={{ (data_interval_start - macros.timedelta(hours=1)).strftime('%H') }}/"
    )


with DAG(
    dag_id="load_cdc_parquet_hourly_to_bigquery",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["cdc", "gcs", "bigquery", "parquet"],
) as dag:

    for topic, table_name in TOPIC_TABLES.items():
        prefix = hourly_prefix(topic)
        uri = f"gs://{BUCKET}/{prefix}*.parquet"

        wait_for_files = GCSObjectsWithPrefixExistenceSensor(
            task_id=f"wait_{table_name}",
            bucket=BUCKET,
            prefix=prefix,
            poke_interval=60,
            timeout=10 * 60,
            mode="reschedule",
        )

        load_to_bq = BigQueryInsertJobOperator(
            task_id=f"load_{table_name}",
            location=LOCATION,
            configuration={
                "load": {
                    "sourceUris": [uri],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": RAW_DATASET,
                        "tableId": table_name,
                    },
                    "sourceFormat": "PARQUET",
                    "writeDisposition": "WRITE_APPEND",
                    "createDisposition": "CREATE_IF_NEEDED",
                    "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"],
                    "autodetect": True,
                }
            },
            job_id=(
                "load_"
                f"{table_name}_"
                "{{ (data_interval_start - macros.timedelta(hours=1)).strftime('%Y%m%d%H') }}"
            ),
            deferrable=True,
        )

        wait_for_files >> load_to_bq
```

Catatan:

- `GCSObjectsWithPrefixExistenceSensor` mengecek ada object di prefix hourly.
- `sourceUris` memakai wildcard `*.parquet`.
- `job_id` dibuat deterministik per table per hour agar lebih idempotent.
- `WRITE_APPEND` cocok karena raw CDC bersifat append-only.
- Jika table tertentu memang tidak ada event di jam tersebut, sensor bisa timeout. Untuk production, buat custom sensor yang skip jika kosong, bukan fail.

---

## Airflow: custom behavior jika tidak ada file

Pada production, beberapa table mungkin tidak berubah setiap jam. Jangan anggap kosong sebagai error.

Pilihan:

1. Buat custom Python task untuk list object GCS.
2. Jika object ada, jalankan BigQuery load.
3. Jika object tidak ada, skip table itu.

Pola ini lebih sehat daripada semua table wajib punya file setiap jam.

---

## MERGE dari raw CDC ke ODS/current

Setelah load ke raw staging, lakukan MERGE.

Contoh untuk `ride`:

```sql
MERGE `my-gcp-project.ride_ods.ride_current` T
USING (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY ride_id
        ORDER BY __source_ts_ms DESC, __lsn DESC
      ) AS row_num
    FROM `my-gcp-project.ride_raw_cdc.raw_ride`
    WHERE __source_ts_ms IS NOT NULL
  )
  WHERE row_num = 1
) S
ON T.ride_id = S.ride_id
WHEN MATCHED AND COALESCE(S.__deleted, false) = true THEN
  UPDATE SET
    T.is_deleted = true,
    T.deleted_at = TIMESTAMP_MILLIS(S.__source_ts_ms),
    T.updated_at = CURRENT_TIMESTAMP()
WHEN MATCHED THEN
  UPDATE SET
    T.rider_id = S.rider_id,
    T.driver_id = S.driver_id,
    T.vehicle_id = S.vehicle_id,
    T.ride_status = S.ride_status,
    T.service_type = S.service_type,
    T.city_code = S.city_code,
    T.requested_at = S.requested_at,
    T.accepted_at = S.accepted_at,
    T.arrived_at = S.arrived_at,
    T.started_at = S.started_at,
    T.completed_at = S.completed_at,
    T.cancelled_at = S.cancelled_at,
    T.cancelled_by_user_id = S.cancelled_by_user_id,
    T.cancel_reason_code = S.cancel_reason_code,
    T.cancel_reason_note = S.cancel_reason_note,
    T.estimated_distance_km = S.estimated_distance_km,
    T.estimated_duration_min = S.estimated_duration_min,
    T.__source_ts_ms = S.__source_ts_ms,
    T.__op = S.__op,
    T.__lsn = S.__lsn,
    T.is_deleted = false,
    T.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND COALESCE(S.__deleted, false) = false THEN
  INSERT (
    ride_id,
    rider_id,
    driver_id,
    vehicle_id,
    ride_status,
    service_type,
    city_code,
    requested_at,
    accepted_at,
    arrived_at,
    started_at,
    completed_at,
    cancelled_at,
    cancelled_by_user_id,
    cancel_reason_code,
    cancel_reason_note,
    estimated_distance_km,
    estimated_duration_min,
    __source_ts_ms,
    __op,
    __lsn,
    is_deleted,
    inserted_at,
    updated_at
  )
  VALUES (
    S.ride_id,
    S.rider_id,
    S.driver_id,
    S.vehicle_id,
    S.ride_status,
    S.service_type,
    S.city_code,
    S.requested_at,
    S.accepted_at,
    S.arrived_at,
    S.started_at,
    S.completed_at,
    S.cancelled_at,
    S.cancelled_by_user_id,
    S.cancel_reason_code,
    S.cancel_reason_note,
    S.estimated_distance_km,
    S.estimated_duration_min,
    S.__source_ts_ms,
    S.__op,
    S.__lsn,
    false,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );
```

Untuk table append-only seperti `ride_status_history` dan `ride_tracking_point`, kamu bisa load langsung ke ODS append table dengan dedup berdasarkan primary key.

---

## Star schema dari ODS

Setelah ODS/current siap, bentuk star schema.

Contoh fact dan dimension:

```text
ride_mart.fact_ride
ride_mart.fact_payment
ride_mart.fact_promo_usage
ride_mart.fact_review
ride_mart.fact_ride_tracking_summary

ride_mart.dim_rider
ride_mart.dim_driver
ride_mart.dim_vehicle
ride_mart.dim_payment_method
ride_mart.dim_promotion
ride_mart.dim_date
ride_mart.dim_time
ride_mart.dim_city
```

Contoh grain:

| Star table | Grain |
|---|---|
| `fact_ride` | 1 baris per ride |
| `fact_payment` | 1 baris per payment transaction |
| `fact_promo_usage` | 1 baris per promo usage |
| `fact_review` | 1 baris per review |
| `fact_ride_tracking_summary` | 1 baris per ride, hasil agregasi GPS |
| `dim_driver` | 1 baris per versi driver jika SCD2 |
| `dim_vehicle` | 1 baris per versi vehicle jika SCD2 |
| `dim_rider` | 1 baris per versi rider jika SCD2 |

Jangan masukkan `ride_tracking_point` mentah langsung sebagai fact utama dashboard kecuali memang butuh analisis GPS detail. Untuk BI, lebih sering dibuat summary:

```text
tracking_point_count
max_speed_kmh
avg_speed_kmh
route_start_latitude
route_start_longitude
route_end_latitude
route_end_longitude
```

---

## ODS vs Star Schema: alur yang benar

```text
raw_cdc.raw_ride
  -> ride_ods.ride_current
  -> ride_mart.fact_ride
```

```text
raw_cdc.raw_driver_profile
  -> ride_ods.driver_profile_current
  -> ride_mart.dim_driver
```

```text
raw_cdc.raw_payment_transaction
  -> ride_ods.payment_transaction_current
  -> ride_mart.fact_payment
```

Dengan pola ini, jika CDC mengirim update atau delete, ODS yang menyelesaikan perubahan row. Star schema membaca state yang sudah bersih.

---

## Folder GCS yang perlu diproses Airflow

Untuk setiap DAG hourly, proses prefix previous hour:

```text
landing/cdc/<topic>/year={{ YYYY }}/month={{ MM }}/day={{ DD }}/hour={{ HH }}/
```

Contoh daftar source URI:

```text
gs://my-ride-lake/landing/cdc/ride_oltp.public.ride/year=2026/month=04/day=30/hour=10/*.parquet
gs://my-ride-lake/landing/cdc/ride_oltp.public.payment_transaction/year=2026/month=04/day=30/hour=10/*.parquet
gs://my-ride-lake/landing/cdc/ride_oltp.public.driver_profile/year=2026/month=04/day=30/hour=10/*.parquet
```

Setelah load sukses, simpan watermark:

```text
ride_audit.cdc_load_watermark
- topic
- gcs_prefix
- target_table
- loaded_at
- airflow_run_id
- bigquery_job_id
- status
```

Ini mencegah load ulang prefix yang sama.

---

## Load banyak file Parquet ke BigQuery

Gunakan wildcard per prefix:

```json
"sourceUris": [
  "gs://my-ride-lake/landing/cdc/ride_oltp.public.ride/year=2026/month=04/day=30/hour=10/*.parquet"
]
```

Jangan list semua file satu per satu kecuali kamu butuh kontrol file-level. Wildcard lebih sederhana dan BigQuery load job memang mendukung source URI dari Cloud Storage.

Hati-hati:

- Jangan load prefix yang masih ditulis oleh connector.
- Load hour sebelumnya, bukan current hour.
- Pastikan semua file dalam wildcard punya schema kompatibel.
- Simpan watermark prefix.
- Jangan hapus file landing sebelum job downstream selesai.

---

## Handling update dan delete

Dengan Debezium unwrap rewrite:

- Insert menjadi row dengan `__op = c` atau `r` saat snapshot.
- Update menjadi row dengan `__op = u`.
- Delete menjadi row dengan `__op = d` dan `__deleted = true`.

Di raw BigQuery, semua event tetap append.

Di ODS/current:

- Untuk `c`, `r`, `u`: upsert row terbaru.
- Untuk `d`: set `is_deleted = true`, jangan langsung hard delete.

Untuk star schema:

- Fact biasanya tidak dihapus fisik. Tandai status atau buat reversal logic.
- Dimension bisa SCD2 jika atribut historis penting.
- Delete dari source harus ditafsirkan sesuai domain. Misalnya `deleted_at` user bukan berarti semua historis ride hilang.

---

## Rekomendasi connector per volume

Untuk volume ringan, satu GCS Sink Connector dengan `topics.regex` cukup.

Untuk production, pisahkan connector berdasarkan volume:

```text
gcs-sink-core-low-volume
- user_account
- driver_profile
- vehicle
- promotion
- review

gcs-sink-ride-volume
- ride
- ride_status_history
- ride_fare
- payment_transaction
- promo_usage

gcs-sink-tracking-high-volume
- ride_tracking_point
```

Kenapa dipisah?

1. `ride_tracking_point` bisa sangat besar.
2. Jika connector tracking bermasalah, core CDC tidak ikut berhenti.
3. Flush size tracking bisa lebih besar.
4. Task parallelism bisa disetel berbeda.

Contoh untuk tracking:

```json
{
  "name": "gcs-sink-tracking-parquet-hourly",
  "config": {
    "connector.class": "io.confluent.connect.gcs.GcsSinkConnector",
    "tasks.max": "4",
    "topics": "ride_oltp.public.ride_tracking_point",
    "gcs.bucket.name": "my-ride-lake",
    "gcs.credentials.path": "/etc/kafka-connect/secrets/gcp-service-account.json",
    "storage.class": "io.confluent.connect.gcs.storage.GcsStorage",
    "format.class": "io.confluent.connect.gcs.format.parquet.ParquetFormat",
    "topics.dir": "landing/cdc",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "partition.duration.ms": "3600000",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "timezone": "UTC",
    "timestamp.extractor": "Wallclock",
    "flush.size": "250000",
    "rotate.schedule.interval.ms": "3600000",
    "behavior.on.null.values": "ignore",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

---

## Airflow task order yang disarankan

```text
1. wait_or_list_gcs_files
2. load_gcs_parquet_to_raw_bigquery
3. record_load_watermark
4. merge_raw_to_ods_current
5. build_dimensions
6. build_facts
7. data_quality_checks
```

Untuk star schema ride-hailing:

```text
load raw_ride
load raw_ride_fare
load raw_payment_transaction
load raw_driver_profile
load raw_vehicle
load raw_promo_usage
        ↓
merge ODS tables
        ↓
build dim_rider, dim_driver, dim_vehicle, dim_promotion
        ↓
build fact_ride, fact_payment, fact_promo_usage
        ↓
BI ready
```

---

## Data quality checks

Tambahkan check minimal:

```sql
-- Tidak boleh ada ride completed tanpa completed_at
SELECT COUNT(*) AS invalid_count
FROM `my-gcp-project.ride_ods.ride_current`
WHERE ride_status = 'COMPLETED'
  AND completed_at IS NULL;
```

```sql
-- Payment paid harus punya paid_at
SELECT COUNT(*) AS invalid_count
FROM `my-gcp-project.ride_ods.payment_transaction_current`
WHERE payment_status = 'PAID'
  AND paid_at IS NULL;
```

```sql
-- Fare final tidak boleh negatif
SELECT COUNT(*) AS invalid_count
FROM `my-gcp-project.ride_ods.ride_fare_current`
WHERE total_fare < 0;
```

---

## Checklist implementasi

1. Aktifkan logical replication PostgreSQL.
2. Buat user Debezium dengan `REPLICATION` dan `SELECT`.
3. Jalankan Kafka, Schema Registry, Kafka Connect.
4. Install plugin Debezium PostgreSQL dan Confluent GCS Sink di Kafka Connect.
5. Register Debezium connector.
6. Pastikan topic muncul dan Avro schema terdaftar.
7. Register GCS Sink connector.
8. Pastikan file Parquet muncul di GCS per topic dan per hour.
9. Buat BigQuery dataset `ride_raw_cdc`, `ride_ods`, `ride_mart`, `ride_audit`.
10. Jalankan Airflow DAG hourly load.
11. Jalankan MERGE ODS.
12. Build star schema mart.
13. Tambahkan data quality checks.

---

## Hal yang sering salah

### 1. Flush hourly disalahartikan

`flush.size` bukan waktu. Itu jumlah record. Untuk file commit tiap jam, gunakan:

```json
"rotate.schedule.interval.ms": "3600000"
```

### 2. Load current hour

Jangan load folder hour yang sedang aktif ditulis connector. Load previous hour dengan buffer.

### 3. Delete event hilang

Jika unwrap tidak disetel dengan benar, delete bisa hilang. Untuk ELT, gunakan:

```json
"transforms.unwrap.delete.tombstone.handling.mode": "rewrite"
```

### 4. Tombstone masuk Parquet

Jika tombstone/null record masuk sink, GCS Sink bisa fail atau file tidak sesuai harapan. Gunakan:

```json
"behavior.on.null.values": "ignore"
```

### 5. Star schema dibangun langsung dari raw CDC

Jangan. Raw CDC masih append-only event. Buat ODS/current dulu.

### 6. Satu connector untuk semua volume

Untuk lab boleh. Untuk production, pisahkan high-volume tracking dari core transactions.

### 7. Tidak ada watermark

Tanpa watermark, Airflow bisa load prefix yang sama berkali-kali. Gunakan audit table.

---

## Rekomendasi akhir untuk project ini

Untuk mulai belajar dengan ride-hailing generator:

```text
Debezium unwrap source connector
Kafka value Avro
GCS Sink Parquet
Hourly folder partition by ingestion time
Airflow load previous hour to BigQuery raw
MERGE raw to ODS
Build star schema from ODS
```

Konfigurasi paling penting:

```text
Debezium:
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.delete.tombstone.handling.mode=rewrite
transforms.unwrap.add.fields=op,table,source.ts_ms,source.db,source.schema,lsn

GCS Sink:
format.class=io.confluent.connect.gcs.format.parquet.ParquetFormat
partitioner.class=io.confluent.connect.storage.partitioner.TimeBasedPartitioner
path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH
rotate.schedule.interval.ms=3600000
flush.size=50000
behavior.on.null.values=ignore

Airflow:
schedule=15 * * * *
load previous hour prefix
sourceFormat=PARQUET
writeDisposition=WRITE_APPEND
job_id deterministic per table-hour
```

Ini cukup kuat untuk lab teknis ELT dan masih masuk akal untuk dikembangkan ke production pattern.
