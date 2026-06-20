# Ride-Hailing Historical Data Generator

Generator ini membuat data historis ride-hailing dalam jumlah besar untuk simulasi analisis bisnis end-to-end:

```text
PostgreSQL/MySQL source → Airflow/Trino → BigQuery → dbt → Looker Studio
```

Target utama: menghasilkan data historis dari `2026-01-01` sampai hari ini, lengkap dengan pola bisnis yang realistis untuk dashboard:

1. Executive Growth & Revenue
2. Marketplace Operations & Funnel
3. Payment Reliability & Refunds
4. Promotion & Customer Experience

---

## 1. Perubahan Utama dari Generator Lama

Generator lama berjalan seperti realtime simulator:

```text
while True → generate_one_ride() → sleep → generate_one_ride()
```

Generator baru menambahkan mode historical:

```text
for date in HIST_START_DATE..HIST_END_DATE
  for hour in 0..23
    generate rides untuk date-hour tersebut
```

Perbedaan utama:

| Aspek | Realtime Lama | Historical Baru |
|---|---|---|
| Mode | infinite loop | selesai otomatis |
| Waktu | mengikuti simulated clock | langsung tanggal historis |
| `sleep` | ada | tidak ada |
| Volume | kecil-menengah | menengah-besar |
| Pattern | realtime random | monthly, weekday, hourly, city, surge, promo, payment |
| Cocok untuk | incremental testing | dashboard, business analysis, ML feature engineering |

---

## 2. File yang Digunakan

Gunakan `generator.py` baru sebagai replacement untuk file lama di folder `data-generator/`.

Struktur yang disarankan:

```text
ride-hailing-docker/
└── data-generator/
    ├── Dockerfile
    ├── generator.py
    ├── init_postgres.sql
    ├── init_mysql.sql
    └── README.md
```

---

## 3. Environment Variables

Contoh `.env` untuk historical mode:

```env
GENERATOR_MODE=historical

HIST_START_DATE=2026-01-01
HIST_END_DATE=2026-06-20
HIST_BASE_RIDES_PER_DAY=2000
HIST_BATCH_SIZE=1000
HIST_RANDOM_SEED=42
HIST_TRUNCATE_BEFORE_LOAD=true
HIST_TRACKING_POINTS_MAX=4
HIST_PROGRESS_EVERY_DAYS=7
HIST_COMMIT_EVERY_DAYS=1
HIST_MONTHLY_GROWTH_RATE=0.055

SEED_RIDERS=50000
SEED_DRIVERS=10000
SEED_PROMOTIONS=60

ENABLE_MAINTENANCE_EVENTS=true
HIST_GENERATE_DRIVER_SHIFTS=true
LOG_LEVEL=INFO

POSTGRES_DSN=postgresql://postgres:postgres@postgres-ops:5432/ride_ops_pg
MYSQL_HOST=mysql-billing
MYSQL_PORT=3306
MYSQL_DATABASE=billing_growth_db
MYSQL_USER=ride_user
MYSQL_PASSWORD=ride_pass
TZ=Asia/Jakarta
```

### Dataset Scale

| Scale | `HIST_BASE_RIDES_PER_DAY` | Estimasi ride 2026-01-01 s/d 2026-06-20 |
|---|---:|---:|
| Small | 500 | ±85.000 |
| Medium | 2.000 | ±340.000 |
| Large | 5.000 | ±850.000 |
| XL | 10.000 | ±1.700.000 |

Rekomendasi awal:

```env
HIST_BASE_RIDES_PER_DAY=2000
```

Jika pipeline sudah stabil, naikkan menjadi:

```env
HIST_BASE_RIDES_PER_DAY=5000
```

---

## 4. Docker Compose Setup

Pastikan service `data-generator` memasukkan `.env` ke container.

### Opsi paling mudah

```yaml
services:
  data-generator:
    build: ./data-generator
    container_name: data-generator
    env_file:
      - .env
```

### Opsi explicit

```yaml
services:
  data-generator:
    build: ./data-generator
    container_name: data-generator
    environment:
      POSTGRES_DSN: postgresql://${POSTGRES_USER:-postgres}:${POSTGRES_PASSWORD:-postgres}@postgres-ops:5432/${POSTGRES_DB:-ride_ops_pg}
      MYSQL_HOST: mysql-billing
      MYSQL_PORT: 3306
      MYSQL_DATABASE: ${MYSQL_DATABASE:-billing_growth_db}
      MYSQL_USER: ${MYSQL_USER:-ride_user}
      MYSQL_PASSWORD: ${MYSQL_PASSWORD:-ride_pass}
      TZ: Asia/Jakarta

      GENERATOR_MODE: ${GENERATOR_MODE:-historical}
      HIST_START_DATE: ${HIST_START_DATE:-2026-01-01}
      HIST_END_DATE: ${HIST_END_DATE:-2026-06-20}
      HIST_BASE_RIDES_PER_DAY: ${HIST_BASE_RIDES_PER_DAY:-2000}
      HIST_BATCH_SIZE: ${HIST_BATCH_SIZE:-1000}
      HIST_RANDOM_SEED: ${HIST_RANDOM_SEED:-42}
      HIST_TRUNCATE_BEFORE_LOAD: ${HIST_TRUNCATE_BEFORE_LOAD:-true}
      HIST_TRACKING_POINTS_MAX: ${HIST_TRACKING_POINTS_MAX:-4}
      SEED_RIDERS: ${SEED_RIDERS:-50000}
      SEED_DRIVERS: ${SEED_DRIVERS:-10000}
      SEED_PROMOTIONS: ${SEED_PROMOTIONS:-60}
      ENABLE_MAINTENANCE_EVENTS: ${ENABLE_MAINTENANCE_EVENTS:-true}
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
```

> Penting: gunakan `MYSQL_DATABASE`, bukan hanya `MYSQL_DB`, karena generator membaca `MYSQL_DATABASE`. Generator tetap menyediakan fallback ke `MYSQL_DB`, tetapi nama yang disarankan adalah `MYSQL_DATABASE`.

---

## 5. Cara Menjalankan

Dari folder project:

```bash
docker compose -f docker-compose-generator.yaml down -v
docker compose -f docker-compose-generator.yaml up -d --build
```

Cek log:

```bash
docker logs -f data-generator
```

Expected log:

```text
Generator starting mode=historical
Truncating source tables before historical load
Seeding master data: riders=50000 drivers=10000 promotions=60
Historical generation started start=2026-01-01 end=2026-06-20 base_rides_per_day=2000
Historical progress day=7/171 date=2026-01-07 generated_today=...
Historical generation completed total_rides=...
Validation rides=... min_date=2026-01-01 max_date=2026-06-20 statuses=...
```

---

## 6. Business Patterns yang Digenerate

Generator ini tidak membuat data random biasa. Data dibuat agar dashboard Looker punya cerita bisnis yang kuat.

---

### 6.1 Monthly Growth Pattern

Demand bertumbuh dari Januari sampai Juni.

Formula konseptual:

```text
rides_per_day = base_rides_per_day × monthly_growth × weekday_multiplier × event_multiplier × noise
```

Default growth:

```env
HIST_MONTHLY_GROWTH_RATE=0.055
```

Artinya demand naik sekitar 5.5% per bulan.

Dampak dashboard:

| Dashboard | Efek |
|---|---|
| Executive Growth & Revenue | revenue dan completed rides naik bertahap |
| Marketplace Operations | volume peak makin tinggi |
| Payment Reliability | payment attempts meningkat |
| Promo & CX | promo usage ikut meningkat |

---

### 6.2 Day-of-Week Pattern

Pattern hari dibuat berbeda:

| Hari | Multiplier | Business Meaning |
|---|---:|---|
| Senin | 1.08 | commuting restart |
| Selasa | 1.00 | baseline |
| Rabu | 0.98 | normal midweek |
| Kamis | 1.02 | normal midweek |
| Jumat | 1.15 | evening/leisure demand naik |
| Sabtu | 1.22 | weekend leisure, CAR/XL naik |
| Minggu | 0.90 | commuting turun |

Dampak dashboard:

| Chart | Insight |
|---|---|
| Surge Effectiveness by Hour | Jumat/Sabtu malam lebih padat |
| Peak vs Non-Peak Performance | weekday morning peak terlihat |
| Promo Rides vs Revenue | weekend promo bisa terlihat |

---

### 6.3 Hourly Demand Pattern

Demand tertinggi dibuat pada:

```text
07:00–09:00 morning commute
17:00–20:00 evening commute
12:00–13:00 lunch moderate
21:00–23:00 leisure moderate
00:00–05:00 low demand
```

Dampak dashboard:

| Chart | Expected Pattern |
|---|---|
| Requested Rides by Hour | spike di 07–09 dan 17–20 |
| Surge Effectiveness | surge naik saat demand peak |
| Cancellation Rate by Hour | cancellation naik saat peak/rain |

---

### 6.4 City Pattern

City distribution:

| City | Base Share | Pattern |
|---|---:|---|
| JKT | 62% | volume terbesar, peak paling kuat |
| BDG | 20% | weekend lebih kuat |
| SBY | 18% | payment incident periodik |

Dampak dashboard:

| Dashboard | Insight |
|---|---|
| Executive Growth & Revenue | JKT menjadi revenue contributor terbesar |
| Marketplace Operations | JKT punya peak pressure tinggi |
| Payment Reliability | SBY punya failure spike jam 19–21 |

---

### 6.5 Service Type Pattern

Service type dibuat mengikuti konteks:

| Service | Pattern |
|---|---|
| BIKE | dominan weekday commute dan JKT peak |
| CAR | naik weekend dan leisure |
| XL | kecil tapi naik weekend/family trip |

Dampak dashboard:

| Chart | Insight |
|---|---|
| Revenue by City-Service | JKT-BIKE volume tinggi, CAR/XL revenue per ride lebih tinggi |
| Funnel Drop-off by City-Service | service tertentu bisa lebih bermasalah |
| Promo Efficiency | WEEKENDCAR bisa terlihat efektif/tidak |

---

### 6.6 Surge Pattern

Surge naik ketika:

```text
peak hour
rain
JKT supply shortage
high demand hour
```

Surge multiplier range:

| Kondisi | Surge |
|---|---:|
| Normal | 1.00–1.10 |
| Peak | 1.15–1.35 |
| Rain | 1.35–1.65 |
| Supply shortage | 1.45–1.80 |

Dampak dashboard:

| Chart | Insight |
|---|---|
| Surge Effectiveness | cek apakah surge menjaga completion atau menaikkan cancellation |
| Completion vs Cancellation Trend | cancellation naik saat surge tinggi/rain |
| Platform Revenue vs Driver Earning | surge menaikkan fare dan platform revenue |

---

### 6.7 Cancellation Pattern

Cancellation tidak random murni. Cancellation naik ketika:

```text
peak hour
rain
accept_delay tinggi
arrival_delay tinggi
supply shortage
surge terlalu tinggi
```

Outcome utama:

| Outcome | Normal | Peak/Rain |
|---|---:|---:|
| COMPLETED | tinggi | turun |
| RIDER_CANCEL_BEFORE_ACCEPT | rendah | naik |
| RIDER_CANCEL_AFTER_ACCEPT | rendah-sedang | naik |
| DRIVER_CANCEL | rendah | naik |
| PAYMENT_FAILED | rendah | naik saat incident |

Dampak dashboard:

| Dashboard | Insight |
|---|---|
| Marketplace Operations & Funnel | drop-off dapat dijelaskan oleh delay/supply |
| Executive Growth & Revenue | growth bisa terlihat tidak sehat kalau completion turun |

---

### 6.8 Payment Incident Pattern

SBY memiliki payment incident periodik:

```text
city = SBY
hour = 19–20
selected incident days
method = EWALLET/CARD
```

Failure code yang mungkin muncul:

```text
INSUFFICIENT_BALANCE
GATEWAY_TIMEOUT
CARD_DECLINED
EWALLET_PROVIDER_DOWN
```

Dampak dashboard:

| Chart | Insight |
|---|---|
| Payment Success Rate Trend | drop pada tanggal/jam tertentu |
| Failure Code Distribution | GATEWAY_TIMEOUT / EWALLET_PROVIDER_DOWN terlihat |
| Payment Method Performance | EWALLET/CARD bisa lebih bermasalah |

---

### 6.9 Promo Campaign Pattern

Promo tidak random murni. Ada campaign khusus:

| Promo Code | Target Pattern |
|---|---|
| COMMUTEBIKE | weekday morning BIKE |
| WEEKENDCAR | weekend CAR/XL |
| PAYDAY25 | tanggal 25–30 |
| JKTBOOST | Jakarta |
| RAINSAFE | rainy evening |
| SBYPAY | Surabaya payment recovery |
| PORTOxxx | filler promo portfolio |

Dampak dashboard:

| Chart | Insight |
|---|---|
| Promo Rides vs Revenue | promo burst awal bulan/payday terlihat |
| Promo Efficiency by Promo Code | promo bisa dibandingkan berdasarkan revenue per discount |
| Discount vs Rating | promo agresif bisa membebani CX |

---

### 6.10 Customer Experience Pattern

Review, ticket, dan refund mengikuti kualitas operasi.

Review dibuat sekitar 10% dari paid rides. Rating buruk lebih mungkin terjadi ketika:

```text
rain
arrival delay tinggi
accept delay tinggi
surge tinggi
supply shortage
```

Jika rating <= 2:

```text
support_ticket dibuat
sebagian ticket menghasilkan refund SERVICE_QUALITY
```

Dampak dashboard:

| Chart | Insight |
|---|---|
| Average Rating | turun saat rain/peak/surge tinggi |
| Support Ticket by Category | keluhan kualitas layanan terlihat |
| Refund Trend by Reason | refund SERVICE_QUALITY terlihat |

---

## 7. Source Tables yang Diisi

### PostgreSQL

| Table | Isi |
|---|---|
| rider_account | master rider |
| driver_profile | master driver |
| vehicle | master vehicle |
| driver_vehicle_assignment | active assignment |
| driver_shift | availability/maintenance event |
| ride | core ride transaction |
| ride_status_history | lifecycle status |
| ride_location | pickup/dropoff requested/actual |
| ride_tracking_point | tracking points |
| ride_fare | fare, surge, revenue, driver earning |

### MySQL

| Table | Isi |
|---|---|
| payment_method | payment method per rider |
| promotion | master promo |
| promo_usage | promo usage per ride |
| payment_transaction | payment attempt/result |
| review | customer rating |
| support_ticket | customer complaint |
| payment_refund | refund |

---

## 8. Data Validation Queries

### PostgreSQL — date coverage

```sql
select
  date(requested_at at time zone 'Asia/Jakarta') as ride_date,
  count(*) as rides
from ride
group by 1
order by 1;
```

Expected:

```text
Ada data dari HIST_START_DATE sampai HIST_END_DATE.
Tidak ada tanggal kosong.
```

---

### PostgreSQL — status distribution

```sql
select
  ride_status,
  count(*) as rides,
  round(count(*) * 100.0 / sum(count(*)) over (), 2) as pct
from ride
group by 1
order by rides desc;
```

Expected:

```text
COMPLETED mayoritas.
CANCELLED terlihat.
PAYMENT_FAILED kecil tapi ada.
```

---

### PostgreSQL — hourly pattern

```sql
select
  extract(hour from requested_at at time zone 'Asia/Jakarta') as requested_hour,
  count(*) as rides
from ride
group by 1
order by 1;
```

Expected:

```text
Spike di 07–09 dan 17–20.
```

---

### PostgreSQL — city-service pattern

```sql
select
  city_code,
  service_type,
  count(*) as rides
from ride
group by 1, 2
order by rides desc;
```

Expected:

```text
JKT terbesar.
BIKE terbesar.
CAR/XL naik saat weekend jika dianalisis per hari.
```

---

### MySQL — payment reliability

```sql
select
  payment_status,
  failure_code,
  count(*) as payments
from payment_transaction
group by 1, 2
order by payments desc;
```

Expected:

```text
PAID mayoritas.
FAILED ada.
Failure code bervariasi.
```

---

### MySQL — promo usage

```sql
select
  p.promo_code,
  count(*) as promo_rides,
  sum(u.discount_amount_applied) as total_discount
from promo_usage u
join promotion p on p.promotion_id = u.promotion_id
group by 1
order by promo_rides desc;
```

Expected:

```text
COMMUTEBIKE, WEEKENDCAR, PAYDAY25, JKTBOOST, RAINSAFE, SBYPAY muncul sesuai pattern.
```

---

## 9. Setelah Generate: Jalankan Pipeline

Karena data source historical biasanya fresh/regenerate, gunakan full refresh di dbt:

```bash
dbt build --full-refresh
```

Urutan pipeline:

```text
1. Generate source data historical
2. Airflow extract Postgres/MySQL to BigQuery raw
3. dbt build --full-refresh
4. Refresh Looker Studio dashboard
```

---

## 10. Rekomendasi Dashboard Looker Setelah Historical Load

### Executive Growth & Revenue

Gunakan untuk melihat:

```text
monthly growth
revenue trend
completed rides trend
city-service contribution
platform take rate
```

### Marketplace Operations & Funnel

Gunakan untuk melihat:

```text
funnel drop-off
peak vs non-peak performance
surge effectiveness
city-service operational issues
```

### Payment Reliability & Refunds

Gunakan untuk melihat:

```text
payment success trend
failure code distribution
payment method performance
refund trend
```

### Promotion & Customer Experience

Gunakan untuk melihat:

```text
promo efficiency
revenue per discount
rating impact
support ticket pattern
refund from service quality
```

---

## 11. Troubleshooting

### Error: env value None

Pastikan `.env` masuk ke container:

```bash
docker exec data-generator env | grep HIST_
```

Jika kosong, tambahkan ini di compose:

```yaml
env_file:
  - .env
```

---

### Error: MySQL database not found

Pastikan env menggunakan:

```env
MYSQL_DATABASE=billing_growth_db
```

Bukan hanya:

```env
MYSQL_DB=billing_growth_db
```

Generator punya fallback ke `MYSQL_DB`, tetapi setting utama yang disarankan adalah `MYSQL_DATABASE`.

---

### Data terlalu besar dan lambat

Turunkan:

```env
HIST_BASE_RIDES_PER_DAY=500
HIST_TRACKING_POINTS_MAX=2
SEED_RIDERS=10000
SEED_DRIVERS=2000
```

---

### Data terlalu kecil

Naikkan:

```env
HIST_BASE_RIDES_PER_DAY=5000
SEED_RIDERS=100000
SEED_DRIVERS=25000
```

---

## 12. Recommended Starter Config

Untuk portfolio Looker yang cukup besar tapi masih aman:

```env
GENERATOR_MODE=historical
HIST_START_DATE=2026-01-01
HIST_END_DATE=2026-06-20
HIST_BASE_RIDES_PER_DAY=2000
HIST_BATCH_SIZE=1000
HIST_RANDOM_SEED=42
HIST_TRUNCATE_BEFORE_LOAD=true
HIST_TRACKING_POINTS_MAX=4
SEED_RIDERS=50000
SEED_DRIVERS=10000
SEED_PROMOTIONS=60
ENABLE_MAINTENANCE_EVENTS=true
LOG_LEVEL=INFO
```

Ini sudah cukup untuk membuat dashboard terlihat realistis, memiliki seasonal/monthly growth, daily/hourly pattern, surge/cancellation problem, payment incident, promo effectiveness, dan CX impact.
