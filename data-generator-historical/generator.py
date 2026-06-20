import logging
import math
import os
import random
import string
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg
import pymysql
from faker import Faker

fake = Faker("id_ID")
JKT = timezone(timedelta(hours=7))

# =========================================================
# Environment helpers
# =========================================================

def env_str(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return default if value is None or value == "" else value


def env_int(name: str, default: int) -> int:
    value = env_str(name, str(default))
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer env {name}={value!r}") from exc


def env_float(name: str, default: float) -> float:
    value = env_str(name, str(default))
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Invalid float env {name}={value!r}") from exc


def env_bool(name: str, default: bool) -> bool:
    value = env_str(name, "true" if default else "false").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def env_date(name: str, default: str) -> date:
    value = env_str(name, default)
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date env {name}={value!r}; expected YYYY-MM-DD") from exc


# =========================================================
# Main configuration
# =========================================================

POSTGRES_DSN = env_str("POSTGRES_DSN", "postgresql://postgres:postgres@postgres-ops:5432/ride_ops_pg")
MYSQL_HOST = env_str("MYSQL_HOST", "mysql-billing")
MYSQL_PORT = env_int("MYSQL_PORT", 3306)
MYSQL_DATABASE = env_str("MYSQL_DATABASE", env_str("MYSQL_DB", "billing_growth_db"))
MYSQL_USER = env_str("MYSQL_USER", "ride_user")
MYSQL_PASSWORD = env_str("MYSQL_PASSWORD", "ride_pass")

GENERATOR_MODE = env_str("GENERATOR_MODE", "historical").lower()
LOG_LEVEL = env_str("LOG_LEVEL", "INFO")
ENABLE_MAINTENANCE_EVENTS = env_bool("ENABLE_MAINTENANCE_EVENTS", True)

# Realtime compatibility settings.
SIM_START_AT = env_str("SIM_START_AT", "2024-01-01T00:00:00+07:00")
SIM_SECONDS_PER_MINUTE = env_float("SIM_SECONDS_PER_MINUTE", 0.05)
RIDES_PER_MINUTE = env_float("RIDES_PER_MINUTE", 10.0)
MAX_CONCURRENT_RIDES = env_int("MAX_CONCURRENT_RIDES", 50)

# Seed settings.
SEED_RIDERS = env_int("SEED_RIDERS", 50000)
SEED_DRIVERS = env_int("SEED_DRIVERS", 10000)
SEED_PROMOTIONS = env_int("SEED_PROMOTIONS", 60)

# Historical settings.
TODAY_JKT = datetime.now(JKT).date()
HIST_START_DATE = env_date("HIST_START_DATE", "2026-01-01")
HIST_END_DATE = env_date("HIST_END_DATE", "2026-03-20")
HIST_BASE_RIDES_PER_DAY = env_int("HIST_BASE_RIDES_PER_DAY", 2000)
HIST_BATCH_SIZE = env_int("HIST_BATCH_SIZE", 1000)
HIST_RANDOM_SEED = env_int("HIST_RANDOM_SEED", 42)
HIST_TRUNCATE_BEFORE_LOAD = env_bool("HIST_TRUNCATE_BEFORE_LOAD", True)
HIST_PROGRESS_EVERY_DAYS = env_int("HIST_PROGRESS_EVERY_DAYS", 7)
HIST_TRACKING_POINTS_MAX = env_int("HIST_TRACKING_POINTS_MAX", 4)
HIST_GENERATE_DRIVER_SHIFTS = env_bool("HIST_GENERATE_DRIVER_SHIFTS", True)
HIST_COMMIT_EVERY_DAYS = env_int("HIST_COMMIT_EVERY_DAYS", 1)

logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ride-hailing-generator")

# =========================================================
# Business constants
# =========================================================

CITY_BOX = {
    "JKT": (-6.35, -6.08, 106.65, 106.95),
    "BDG": (-6.98, -6.82, 107.55, 107.75),
    "SBY": (-7.35, -7.20, 112.65, 112.85),
}

CITY_WEIGHTS = {"JKT": 0.62, "BDG": 0.20, "SBY": 0.18}

# Hourly profile is intentionally not normalized. It is normalized during allocation.
# 07-09 and 17-20 become demand peaks.
HOURLY_WEIGHTS = {
    0: 0.20, 1: 0.12, 2: 0.08, 3: 0.06, 4: 0.08, 5: 0.20,
    6: 0.70, 7: 1.45, 8: 1.75, 9: 1.35, 10: 0.85, 11: 0.90,
    12: 1.05, 13: 0.85, 14: 0.78, 15: 0.82, 16: 1.00, 17: 1.65,
    18: 1.95, 19: 1.70, 20: 1.30, 21: 0.95, 22: 0.65, 23: 0.38,
}

# Python date.weekday(): Monday=0, Sunday=6.
WEEKDAY_MULTIPLIERS = {
    0: 1.08,  # Monday commute restart
    1: 1.00,
    2: 0.98,
    3: 1.02,
    4: 1.15,  # Friday evening
    5: 1.22,  # Saturday leisure
    6: 0.90,  # Sunday lower commuting
}

MONTHLY_GROWTH_PER_MONTH = env_float("HIST_MONTHLY_GROWTH_RATE", 0.055)


@dataclass(frozen=True)
class Driver:
    driver_id: int
    vehicle_id: int
    vehicle_type: str
    city_code: str


@dataclass(frozen=True)
class PaymentMethod:
    payment_method_id: int
    method_code: str
    provider_name: str


@dataclass(frozen=True)
class Promo:
    promotion_id: int
    promo_code: str
    discount_type: str
    discount_pct: Decimal
    discount_amount: Decimal
    max_discount_amount: Decimal
    min_fare_amount: Decimal
    valid_from: datetime
    valid_to: datetime


class SimClock:
    """Realtime mode clock. Historical mode does not use sleep."""

    def __init__(self, start_at: str, sim_seconds_per_minute: float):
        dt = datetime.fromisoformat(start_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JKT)
        self.sim_start = dt.astimezone(JKT)
        self.real_start = datetime.now(JKT)
        self.sim_seconds_per_minute = float(sim_seconds_per_minute)

    def now(self) -> datetime:
        real_elapsed = (datetime.now(JKT) - self.real_start).total_seconds()
        sim_minutes = real_elapsed / self.sim_seconds_per_minute
        return self.sim_start + timedelta(minutes=sim_minutes)

    def sleep_sim_minutes(self, minutes: float) -> None:
        time.sleep(max(0.02, minutes * self.sim_seconds_per_minute))


# =========================================================
# Database helpers
# =========================================================

def pg_conn():
    return psycopg.connect(POSTGRES_DSN)


def my_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        autocommit=False,
    )


def fetch_one_pg(conn, sql: str, params: Optional[tuple] = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        return row[0] if row else None


def fetch_all_pg(conn, sql: str, params: Optional[tuple] = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def fetch_all_mysql(conn, sql: str, params: Optional[tuple] = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def execute_many_pg(conn, sql: str, rows: Sequence[tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def execute_many_mysql(conn, sql: str, rows: Sequence[tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


# =========================================================
# Generic helpers
# =========================================================

def money(value) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def mysql_dt(dt: Optional[datetime]):
    if dt is None:
        return None
    return dt.astimezone(JKT).replace(tzinfo=None)


def pg_dt(dt: datetime) -> datetime:
    return dt.astimezone(JKT)


def daterange(start_date: date, end_date: date) -> Iterable[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def plate(i: Optional[int] = None) -> str:
    digits = random.randint(1000, 9999) if i is None else 1000 + (i % 9000)
    suffix = "".join(random.choices(string.ascii_uppercase, k=random.choice([2, 3])))
    return f"B {digits} {suffix}"


def rand_point(city: str) -> Tuple[float, float]:
    lat_min, lat_max, lon_min, lon_max = CITY_BOX[city]
    return round(random.uniform(lat_min, lat_max), 6), round(random.uniform(lon_min, lon_max), 6)


def random_datetime_in_hour(d: date, hour: int) -> datetime:
    return datetime.combine(d, dt_time(hour, random.randint(0, 59), random.randint(0, 59), tzinfo=JKT))


def weighted_choice(weight_map: Dict[str, float]) -> str:
    keys = list(weight_map.keys())
    vals = list(weight_map.values())
    return random.choices(keys, weights=vals, k=1)[0]


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


# =========================================================
# Business pattern logic
# =========================================================

def month_index(d: date) -> int:
    return (d.year - HIST_START_DATE.year) * 12 + (d.month - HIST_START_DATE.month)


def monthly_growth_multiplier(d: date) -> float:
    return 1.0 + (MONTHLY_GROWTH_PER_MONTH * max(0, month_index(d)))


def special_event_multiplier(d: date) -> float:
    """Create a few realistic non-random calendar patterns."""
    m = 1.0
    # Pay-day / month-end demand.
    if d.day in {25, 26, 27, 28, 29, 30}:
        m *= 1.10
    # Beginning-of-month promo burst.
    if d.day in {1, 2, 3}:
        m *= 1.07
    # Mid-month demand softness.
    if d.day in {13, 14, 15}:
        m *= 0.94
    return m


def rides_per_day(d: date) -> int:
    base = HIST_BASE_RIDES_PER_DAY
    weekly = WEEKDAY_MULTIPLIERS[d.weekday()]
    monthly = monthly_growth_multiplier(d)
    event = special_event_multiplier(d)
    noise = random.uniform(0.92, 1.08)
    return max(1, int(base * weekly * monthly * event * noise))


def rides_per_hour(total_day_rides: int, d: date, hour: int) -> int:
    weight_sum = sum(HOURLY_WEIGHTS.values())
    hour_base = total_day_rides * (HOURLY_WEIGHTS[hour] / weight_sum)
    # Friday night and Saturday evening leisure boost.
    if d.weekday() == 4 and hour in range(17, 22):
        hour_base *= 1.16
    if d.weekday() == 5 and hour in range(10, 22):
        hour_base *= 1.12
    if d.weekday() == 6 and hour in range(8, 12):
        hour_base *= 0.85
    noise = random.uniform(0.85, 1.15)
    return max(0, int(hour_base * noise))


def choose_city(now: datetime) -> str:
    hour = now.hour
    weights = CITY_WEIGHTS.copy()
    if hour in range(7, 10) or hour in range(17, 21):
        weights["JKT"] += 0.10
        weights["BDG"] -= 0.04
        weights["SBY"] -= 0.06
    if now.weekday() == 5:
        weights["BDG"] += 0.04
        weights["JKT"] -= 0.02
        weights["SBY"] -= 0.02
    return weighted_choice(weights)


def choose_service(now: datetime, city: str) -> str:
    hour = now.hour
    is_weekend = now.weekday() >= 5
    base = {"BIKE": 0.58, "CAR": 0.34, "XL": 0.08}
    if hour in range(7, 10):
        base["BIKE"] += 0.12
        base["CAR"] -= 0.08
        base["XL"] -= 0.04
    if is_weekend:
        base["BIKE"] -= 0.10
        base["CAR"] += 0.06
        base["XL"] += 0.04
    if city == "JKT" and hour in range(17, 21):
        base["BIKE"] += 0.06
        base["CAR"] -= 0.02
        base["XL"] -= 0.04
    return weighted_choice(base)


def business_context(now: datetime, city: str) -> Dict[str, object]:
    hour = now.hour
    is_peak = hour in range(7, 10) or hour in range(17, 21)
    is_weekend = now.weekday() >= 5
    # Jakarta rain is common in late afternoon. Jan-Feb intentionally stronger.
    rainy_month_boost = now.month in {1, 2, 3}
    rain_prob = 0.34 if rainy_month_boost else 0.22
    rain = city == "JKT" and hour in range(16, 19) and random.random() < rain_prob
    # SBY payment incident creates visible daily/hourly payment pattern.
    payment_incident_day = now.day in {6, 17, 28} or (now.weekday() == 4 and now.day % 2 == 0)
    payment_incident = city == "SBY" and hour in range(19, 21) and payment_incident_day
    promo_boost = now.day % 10 in {1, 2, 3} or now.day in {25, 26, 27, 28}
    supply_shortage = is_peak and city == "JKT" and random.random() < (0.28 if rain else 0.16)
    return {
        "is_peak": is_peak,
        "is_weekend": is_weekend,
        "rain": rain,
        "payment_incident": payment_incident,
        "promo_boost": promo_boost,
        "supply_shortage": supply_shortage,
    }


def fare_breakdown(distance_km, duration_min, service_type: str, context: Dict[str, object], discount=Decimal("0.00")) -> Dict[str, Decimal]:
    if service_type == "BIKE":
        base, per_km, per_min = 7000, 2400, 250
    elif service_type == "XL":
        base, per_km, per_min = 15000, 5200, 600
    else:
        base, per_km, per_min = 10000, 3800, 450

    surge_choices = [1.0, 1.0, 1.0, 1.05, 1.1]
    if context["is_peak"]:
        surge_choices += [1.15, 1.25, 1.35]
    if context["rain"]:
        surge_choices += [1.35, 1.50, 1.65]
    if context["supply_shortage"]:
        surge_choices += [1.45, 1.60, 1.80]

    surge_multiplier = Decimal(str(random.choice(surge_choices))).quantize(Decimal("0.01"))
    base_fare = money(base)
    distance_fare = money(float(distance_km) * per_km)
    time_fare = money(float(duration_min) * per_min)
    before_surge = base_fare + distance_fare + time_fare
    surge_amount = money(before_surge * (surge_multiplier - Decimal("1.00")))
    taxable = max(Decimal("0"), before_surge + surge_amount - discount)
    tax_amount = money(taxable * Decimal("0.11"))
    total_fare = money(taxable + tax_amount)
    platform_fee = money((before_surge + surge_amount) * Decimal("0.18"))
    driver_earning = money(max(Decimal("0"), total_fare - platform_fee))
    return {
        "base_fare": base_fare,
        "distance_fare": distance_fare,
        "time_fare": time_fare,
        "surge_multiplier": surge_multiplier,
        "surge_amount": surge_amount,
        "discount_amount": money(discount),
        "tax_amount": tax_amount,
        "platform_fee": platform_fee,
        "driver_earning": driver_earning,
        "total_fare": total_fare,
    }


def choose_outcome(context: Dict[str, object], accept_delay: float, arrive_delay: float, preliminary_surge: Decimal) -> str:
    weights = {
        "COMPLETED": 84.0,
        "RIDER_CANCEL_BEFORE_ACCEPT": 4.0,
        "RIDER_CANCEL_AFTER_ACCEPT": 5.0,
        "DRIVER_CANCEL": 3.0,
        "PAYMENT_FAILED": 4.0,
    }
    if context["is_peak"]:
        weights["COMPLETED"] -= 3.0
        weights["RIDER_CANCEL_AFTER_ACCEPT"] += 1.8
        weights["DRIVER_CANCEL"] += 1.2
    if context["rain"]:
        weights["COMPLETED"] -= 7.0
        weights["RIDER_CANCEL_AFTER_ACCEPT"] += 3.5
        weights["DRIVER_CANCEL"] += 3.5
    if context["supply_shortage"]:
        weights["COMPLETED"] -= 6.0
        weights["RIDER_CANCEL_BEFORE_ACCEPT"] += 2.0
        weights["DRIVER_CANCEL"] += 4.0
    if context["payment_incident"]:
        weights["PAYMENT_FAILED"] += 9.0
        weights["COMPLETED"] -= 9.0
    if accept_delay > 6.5:
        weights["RIDER_CANCEL_BEFORE_ACCEPT"] += 2.5
        weights["COMPLETED"] -= 2.5
    if arrive_delay > 16:
        weights["RIDER_CANCEL_AFTER_ACCEPT"] += 2.5
        weights["COMPLETED"] -= 2.5
    if preliminary_surge >= Decimal("1.50"):
        weights["RIDER_CANCEL_BEFORE_ACCEPT"] += 1.0
        weights["RIDER_CANCEL_AFTER_ACCEPT"] += 1.2
        weights["COMPLETED"] -= 2.2
    # Avoid negative weights after adjustments.
    weights = {k: max(0.5, v) for k, v in weights.items()}
    return weighted_choice(weights)


# =========================================================
# Seed and reset
# =========================================================

def truncate_all_sources() -> None:
    logger.warning("Truncating source tables before historical load")
    with pg_conn() as pg:
        with pg.cursor() as cur:
            cur.execute(
                """
                truncate table
                    ride_tracking_point,
                    ride_location,
                    ride_status_history,
                    ride_fare,
                    ride,
                    driver_shift,
                    driver_vehicle_assignment,
                    vehicle,
                    driver_profile,
                    rider_account
                restart identity cascade
                """
            )
        pg.commit()
    with my_conn() as my:
        with my.cursor() as cur:
            for table in [
                "payment_refund",
                "promo_usage",
                "payment_transaction",
                "review",
                "support_ticket",
                "payment_method",
                "promotion",
            ]:
                cur.execute(f"truncate table {table}")
        my.commit()


def seed_sources(base_date: Optional[datetime] = None) -> None:
    base = base_date or datetime.combine(HIST_START_DATE, dt_time(0, 0, tzinfo=JKT)) - timedelta(days=90)
    with pg_conn() as pg, my_conn() as my:
        existing = fetch_one_pg(pg, "select count(*) from rider_account")
        if existing and existing >= SEED_RIDERS:
            logger.info("Seed already exists: riders=%s", existing)
            return

        logger.info("Seeding master data: riders=%s drivers=%s promotions=%s", SEED_RIDERS, SEED_DRIVERS, SEED_PROMOTIONS)
        with pg.cursor() as cur:
            rider_rows = []
            for i in range(SEED_RIDERS):
                city = random.choices(list(CITY_WEIGHTS), weights=list(CITY_WEIGHTS.values()), k=1)[0]
                created = base + timedelta(minutes=i * random.randint(2, 8))
                rider_rows.append((
                    f"rider_{i + 1:06d}",
                    fake.name(),
                    f"rider_{i + 1:06d}@example.test",
                    f"+62813{10000000 + i:08d}",
                    city,
                    created,
                    created,
                ))
            cur.executemany(
                """
                insert into rider_account(username, full_name, email, phone_number, account_status, city_code, created_at, updated_at)
                values(%s,%s,%s,%s,'ACTIVE',%s,%s,%s)
                on conflict(username) do nothing
                """,
                rider_rows,
            )

            makes = {
                "BIKE": [("Yamaha", "NMAX"), ("Honda", "Vario"), ("Honda", "Beat")],
                "CAR": [("Toyota", "Avanza"), ("Honda", "Brio"), ("Daihatsu", "Xenia")],
                "XL": [("Toyota", "Innova"), ("Toyota", "Voxy")],
            }
            for i in range(SEED_DRIVERS):
                city = random.choices(list(CITY_WEIGHTS), weights=list(CITY_WEIGHTS.values()), k=1)[0]
                vtype = random.choices(["BIKE", "CAR", "XL"], weights=[0.55, 0.36, 0.09], k=1)[0]
                created = base + timedelta(minutes=i * random.randint(5, 15))
                cur.execute(
                    """
                    insert into driver_profile(driver_name, phone_number, city_code, driver_status, verification_status, rating_avg, rating_count, created_at, updated_at)
                    values(%s,%s,%s,'AVAILABLE','VERIFIED',%s,%s,%s,%s)
                    returning driver_id
                    """,
                    (fake.name(), f"+62817{10000000 + i:08d}", city, money(random.uniform(4.55, 5.00)), random.randint(10, 500), created, created),
                )
                driver_id = cur.fetchone()[0]
                make, model = random.choice(makes[vtype])
                cur.execute(
                    """
                    insert into vehicle(driver_id, license_plate, vehicle_type, vehicle_make, vehicle_model, vehicle_year, vehicle_status, created_at, updated_at)
                    values(%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s)
                    returning vehicle_id
                    """,
                    (driver_id, plate(i), vtype, make, model, random.randint(2016, 2025), created, created),
                )
                vehicle_id = cur.fetchone()[0]
                cur.execute(
                    """
                    insert into driver_vehicle_assignment(driver_id, vehicle_id, assigned_from, is_active, created_at, updated_at)
                    values(%s,%s,%s,true,%s,%s)
                    """,
                    (driver_id, vehicle_id, created, created, created),
                )
        pg.commit()

        with my.cursor() as cur:
            payment_rows = []
            for rider_id in range(1, SEED_RIDERS + 1):
                n_methods = random.choice([1, 1, 1, 2])
                for j in range(n_methods):
                    method = random.choices(["EWALLET", "CARD", "BANK_TRANSFER", "CASH"], weights=[45, 25, 15, 15], k=1)[0]
                    provider = {"EWALLET": "demo_ewallet", "CARD": "demo_card", "BANK_TRANSFER": "demo_bank", "CASH": "cash"}[method]
                    payment_rows.append((rider_id, method, provider, "****" + str(random.randint(1000, 9999)), j == 0, mysql_dt(base), mysql_dt(base)))
            cur.executemany(
                """
                insert into payment_method(rider_id, method_code, provider_name, masked_account, payment_method_status, is_default, created_at, updated_at)
                values(%s,%s,%s,%s,'ACTIVE',%s,%s,%s)
                """,
                payment_rows,
            )

            promo_rows = build_promotion_rows(base)
            cur.executemany(
                """
                insert into promotion(promo_code, promo_description, discount_type, discount_pct, discount_amount, max_discount_amount, min_fare_amount, valid_from, valid_to, promotion_status, created_at, updated_at)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s)
                """,
                promo_rows,
            )
        my.commit()
        logger.info("Seed completed")


def build_promotion_rows(base: datetime) -> List[tuple]:
    valid_from = mysql_dt(datetime.combine(HIST_START_DATE, dt_time(0, 0, tzinfo=JKT)) - timedelta(days=10))
    valid_to = mysql_dt(datetime.combine(HIST_END_DATE, dt_time(23, 59, 59, tzinfo=JKT)) + timedelta(days=30))
    fixed_campaigns = [
        ("COMMUTEBIKE", "Weekday morning bike commute booster", "PERCENT", Decimal("10.00"), None, Decimal("12000.00"), Decimal("25000.00")),
        ("WEEKENDCAR", "Weekend car and XL leisure booster", "FIXED", None, Decimal("15000.00"), Decimal("15000.00"), Decimal("50000.00")),
        ("PAYDAY25", "Payday demand capture campaign", "PERCENT", Decimal("15.00"), None, Decimal("25000.00"), Decimal("40000.00")),
        ("JKTBOOST", "Jakarta high-density city campaign", "PERCENT", Decimal("8.00"), None, Decimal("15000.00"), Decimal("30000.00")),
        ("RAINSAFE", "Rainy evening retention campaign", "FIXED", None, Decimal("10000.00"), Decimal("10000.00"), Decimal("30000.00")),
        ("SBYPAY", "Surabaya payment recovery campaign", "PERCENT", Decimal("12.00"), None, Decimal("18000.00"), Decimal("35000.00")),
    ]
    rows = []
    for code, desc, dtype, pct, fixed, max_disc, min_fare in fixed_campaigns[:SEED_PROMOTIONS]:
        rows.append((code, desc, dtype, money(pct) if pct is not None else None, money(fixed) if fixed is not None else None, money(max_disc), money(min_fare), valid_from, valid_to, mysql_dt(base), mysql_dt(base)))
    for i in range(max(0, SEED_PROMOTIONS - len(rows))):
        dtype = random.choice(["PERCENT", "FIXED"])
        code = f"PORTO{i + 1:03d}"
        rows.append((
            code,
            f"Portfolio promo {i + 1}",
            dtype,
            money(random.choice([5, 8, 10, 12, 15, 20])) if dtype == "PERCENT" else None,
            money(random.choice([5000, 10000, 15000, 20000])) if dtype == "FIXED" else None,
            money(random.choice([12000, 18000, 25000, 30000])),
            money(random.choice([20000, 30000, 40000, 50000])),
            valid_from,
            valid_to,
            mysql_dt(base),
            mysql_dt(base),
        ))
    return rows


# =========================================================
# Loading master lookup data
# =========================================================

def load_riders_by_city() -> Dict[str, List[int]]:
    with pg_conn() as conn:
        rows = fetch_all_pg(conn, "select rider_id, city_code from rider_account where account_status='ACTIVE' and deleted_at is null")
    result = {city: [] for city in CITY_WEIGHTS}
    all_riders = []
    for rider_id, city in rows:
        all_riders.append(int(rider_id))
        result.setdefault(city, []).append(int(rider_id))
    result["ALL"] = all_riders
    return result


def load_drivers_by_city_service() -> Dict[Tuple[str, str], List[Driver]]:
    with pg_conn() as conn:
        rows = fetch_all_pg(
            conn,
            """
            select d.driver_id, v.vehicle_id, v.vehicle_type, d.city_code
            from driver_profile d
            join vehicle v on v.driver_id = d.driver_id and v.vehicle_status='ACTIVE' and v.deleted_at is null
            where d.verification_status='VERIFIED' and d.deleted_at is null
            """,
        )
    result: Dict[Tuple[str, str], List[Driver]] = {}
    for row in rows:
        driver = Driver(int(row[0]), int(row[1]), row[2], row[3])
        result.setdefault((driver.city_code, driver.vehicle_type), []).append(driver)
    return result


def load_payment_methods_by_rider() -> Dict[int, List[PaymentMethod]]:
    with my_conn() as conn:
        rows = fetch_all_mysql(
            conn,
            """
            select payment_method_id, rider_id, method_code, provider_name
            from payment_method
            where payment_method_status='ACTIVE' and deleted_at is null
            order by rider_id, is_default desc, payment_method_id
            """,
        )
    result: Dict[int, List[PaymentMethod]] = {}
    for payment_method_id, rider_id, method_code, provider_name in rows:
        result.setdefault(int(rider_id), []).append(PaymentMethod(int(payment_method_id), method_code, provider_name))
    return result


def load_promos() -> List[Promo]:
    with my_conn() as conn:
        rows = fetch_all_mysql(
            conn,
            """
            select promotion_id, promo_code, discount_type, coalesce(discount_pct,0), coalesce(discount_amount,0),
                   coalesce(max_discount_amount,0), coalesce(min_fare_amount,0), valid_from, valid_to
            from promotion
            where promotion_status='ACTIVE' and deleted_at is null
            """,
        )
    promos = []
    for row in rows:
        promos.append(Promo(
            promotion_id=int(row[0]),
            promo_code=row[1],
            discount_type=row[2],
            discount_pct=money(row[3]),
            discount_amount=money(row[4]),
            max_discount_amount=money(row[5]),
            min_fare_amount=money(row[6]),
            valid_from=row[7],
            valid_to=row[8],
        ))
    return promos


def next_payment_transaction_id_start() -> int:
    with my_conn() as conn:
        row = fetch_all_mysql(conn, "select coalesce(max(payment_transaction_id), 0) + 1 from payment_transaction")
        return int(row[0][0]) if row else 1


def reserve_ride_ids(conn, n: int) -> List[int]:
    if n <= 0:
        return []
    with conn.cursor() as cur:
        cur.execute("select nextval(pg_get_serial_sequence('ride', 'ride_id')) from generate_series(1, %s)", (n,))
        return [int(row[0]) for row in cur.fetchall()]


# =========================================================
# Promo/payment/review logic
# =========================================================

def apply_promo_for_historical(rider_id: int, preliminary_fare: Decimal, context: Dict[str, object], completed_at: datetime, city: str, service_type: str, promos: List[Promo]) -> Tuple[Decimal, Optional[Promo]]:
    completed_naive = mysql_dt(completed_at)
    eligible = [p for p in promos if p.valid_from <= completed_naive <= p.valid_to]
    if not eligible:
        return Decimal("0.00"), None

    chance = 0.13
    if context["promo_boost"]:
        chance += 0.17
    if context["is_weekend"] and service_type in {"CAR", "XL"}:
        chance += 0.10
    if service_type == "BIKE" and completed_at.weekday() < 5 and completed_at.hour in range(7, 10):
        chance += 0.08
    if city == "JKT":
        chance += 0.03
    if random.random() > min(chance, 0.45):
        return Decimal("0.00"), None

    # Weighted campaign selection for business pattern.
    campaign_candidates = []
    for p in eligible:
        weight = 1.0
        code = p.promo_code.upper()
        if code == "COMMUTEBIKE" and service_type == "BIKE" and completed_at.weekday() < 5 and completed_at.hour in range(7, 10):
            weight = 8.0
        elif code == "WEEKENDCAR" and context["is_weekend"] and service_type in {"CAR", "XL"}:
            weight = 8.0
        elif code == "PAYDAY25" and completed_at.day in {25, 26, 27, 28, 29, 30}:
            weight = 7.0
        elif code == "JKTBOOST" and city == "JKT":
            weight = 5.0
        elif code == "RAINSAFE" and context["rain"]:
            weight = 6.0
        elif code == "SBYPAY" and city == "SBY":
            weight = 5.0
        campaign_candidates.append((p, weight))

    promo = random.choices([p for p, _ in campaign_candidates], weights=[w for _, w in campaign_candidates], k=1)[0]
    if preliminary_fare < promo.min_fare_amount:
        return Decimal("0.00"), None
    if promo.discount_type == "PERCENT":
        discount = min(money(preliminary_fare * promo.discount_pct / Decimal("100")), promo.max_discount_amount)
    else:
        discount = min(promo.discount_amount, promo.max_discount_amount)
    return money(discount), promo


def choose_payment_method(rider_id: int, payment_methods: Dict[int, List[PaymentMethod]]) -> PaymentMethod:
    methods = payment_methods.get(rider_id)
    if not methods:
        return PaymentMethod(0, "CASH", "cash")
    # Default method is first because loaded ordered by default desc.
    return random.choice([methods[0], methods[0]] + methods)


def payment_status_for(method: PaymentMethod, outcome: str, context: Dict[str, object]) -> Tuple[str, Optional[str]]:
    incident_failed = context["payment_incident"] and method.method_code in {"EWALLET", "CARD"}
    if outcome == "PAYMENT_FAILED" or incident_failed:
        failure_codes = ["INSUFFICIENT_BALANCE", "GATEWAY_TIMEOUT", "CARD_DECLINED", "EWALLET_PROVIDER_DOWN"] if incident_failed else ["INSUFFICIENT_BALANCE", "GATEWAY_TIMEOUT", "CARD_DECLINED"]
        return "FAILED", random.choice(failure_codes)
    # Normal small failure rate.
    failed = random.random() < 0.035
    if failed:
        return "FAILED", random.choice(["INSUFFICIENT_BALANCE", "GATEWAY_TIMEOUT", "CARD_DECLINED"])
    return "PAID", None


def choose_rating(context: Dict[str, object], accept_delay: float, arrive_delay: float, surge_multiplier: Decimal) -> int:
    weights = [2, 2, 7, 20, 69]  # 1..5
    if context["rain"] or arrive_delay > 18 or accept_delay > 8 or surge_multiplier >= Decimal("1.50"):
        weights = [4, 5, 12, 25, 54]
    if context["supply_shortage"]:
        weights = [5, 6, 13, 26, 50]
    return random.choices([1, 2, 3, 4, 5], weights=weights, k=1)[0]


# =========================================================
# Historical batch generator
# =========================================================

class HistoricalBatch:
    def __init__(self) -> None:
        self.pg_ride_rows: List[tuple] = []
        self.pg_status_rows: List[tuple] = []
        self.pg_location_rows: List[tuple] = []
        self.pg_tracking_rows: List[tuple] = []
        self.pg_fare_rows: List[tuple] = []
        self.pg_shift_rows: List[tuple] = []

        self.my_payment_rows: List[tuple] = []
        self.my_promo_rows: List[tuple] = []
        self.my_review_rows: List[tuple] = []
        self.my_ticket_rows: List[tuple] = []
        self.my_refund_rows: List[tuple] = []

    def clear(self) -> None:
        self.__init__()

    @property
    def ride_count(self) -> int:
        return len(self.pg_ride_rows)


def insert_batch(pg, my, batch: HistoricalBatch) -> None:
    execute_many_pg(
        pg,
        """
        insert into ride(ride_id, rider_id, driver_id, vehicle_id, ride_status, service_type, city_code, requested_at, accepted_at, arrived_at, started_at, completed_at, cancelled_at, cancelled_by_type, cancel_reason_code, estimated_distance_km, estimated_duration_min, created_at, updated_at)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.pg_ride_rows,
    )
    execute_many_pg(
        pg,
        """
        insert into ride_status_history(ride_id, old_status, new_status, changed_by_type, changed_by_id, reason_code, reason_note, changed_at, created_at)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.pg_status_rows,
    )
    execute_many_pg(
        pg,
        """
        insert into ride_location(ride_id, location_type, latitude, longitude, address_text, captured_at, created_at)
        values(%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.pg_location_rows,
    )
    execute_many_pg(
        pg,
        """
        insert into ride_tracking_point(ride_id, driver_id, latitude, longitude, speed_kmh, recorded_at, created_at)
        values(%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.pg_tracking_rows,
    )
    execute_many_pg(
        pg,
        """
        insert into ride_fare(ride_id, fare_type, fare_version, distance_km, duration_min, base_fare, distance_fare, time_fare, surge_multiplier, surge_amount, discount_amount, tax_amount, platform_fee, driver_earning, total_fare, fare_rule_code, calculated_at, created_at, updated_at)
        values(%s,'FINAL',1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.pg_fare_rows,
    )
    execute_many_pg(
        pg,
        """
        insert into driver_shift(driver_id, shift_status, started_at, ended_at, created_at, updated_at)
        values(%s,%s,%s,%s,%s,%s)
        """,
        batch.pg_shift_rows,
    )

    execute_many_mysql(
        my,
        """
        insert into payment_transaction(payment_transaction_id, ride_id, rider_id, payment_method_id, provider_name, provider_transaction_id, idempotency_key, amount, method_fee, payment_status, failure_code, failure_message, authorized_at, captured_at, paid_at, created_at, updated_at)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.my_payment_rows,
    )
    execute_many_mysql(
        my,
        """
        insert ignore into promo_usage(promotion_id, ride_id, rider_id, discount_amount_applied, used_at, created_at, updated_at)
        values(%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.my_promo_rows,
    )
    execute_many_mysql(
        my,
        """
        insert into review(ride_id, reviewer_type, reviewer_id, reviewee_type, reviewee_id, rating_score, comments, review_status, created_at, updated_at, deleted_at)
        values(%s,'RIDER',%s,'DRIVER',%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.my_review_rows,
    )
    execute_many_mysql(
        my,
        """
        insert into support_ticket(ride_id, rider_id, driver_id, ticket_category, ticket_status, priority, opened_at, resolved_at, created_at, updated_at)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.my_ticket_rows,
    )
    execute_many_mysql(
        my,
        """
        insert into payment_refund(payment_transaction_id, ride_id, refund_amount, refund_reason_code, refund_status, requested_at, processed_at, created_at, updated_at)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        batch.my_refund_rows,
    )


def add_status(batch: HistoricalBatch, ride_id: int, old_status: Optional[str], new_status: str, who_type: str, who_id: Optional[int], changed_at: datetime, reason: Optional[str] = None, note: Optional[str] = None) -> None:
    batch.pg_status_rows.append((ride_id, old_status, new_status, who_type, who_id, reason, note, pg_dt(changed_at), pg_dt(changed_at)))


def add_driver_shifts(batch: HistoricalBatch, d: date, drivers: Dict[Tuple[str, str], List[Driver]]) -> None:
    if not ENABLE_MAINTENANCE_EVENTS or not HIST_GENERATE_DRIVER_SHIFTS:
        return
    for driver_list in drivers.values():
        # sample a tiny subset to demonstrate driver availability events without exploding rows
        sample_size = max(1, int(len(driver_list) * 0.004))
        for driver in random.sample(driver_list, min(sample_size, len(driver_list))):
            start = datetime.combine(d, dt_time(random.randint(5, 19), random.randint(0, 59), tzinfo=JKT))
            status = random.choices(["AVAILABLE", "OFFLINE", "SUSPENDED"], weights=[75, 22, 3], k=1)[0]
            ended = start + timedelta(hours=random.choice([2, 4, 6, 8])) if status == "OFFLINE" else None
            batch.pg_shift_rows.append((driver.driver_id, status, pg_dt(start), pg_dt(ended) if ended else None, pg_dt(start), pg_dt(start)))


def generate_historical_ride(
    batch: HistoricalBatch,
    ride_id: int,
    requested_at: datetime,
    riders_by_city: Dict[str, List[int]],
    drivers_by_city_service: Dict[Tuple[str, str], List[Driver]],
    payment_methods: Dict[int, List[PaymentMethod]],
    promos: List[Promo],
    payment_id_counter: List[int],
) -> None:
    city = choose_city(requested_at)
    service_type = choose_service(requested_at, city)
    context = business_context(requested_at, city)

    riders = riders_by_city.get(city) or riders_by_city["ALL"]
    driver_pool = drivers_by_city_service.get((city, service_type))
    if not riders or not driver_pool:
        logger.warning("Missing pool city=%s service=%s riders=%s drivers=%s", city, service_type, len(riders), len(driver_pool or []))
        return

    rider_id = random.choice(riders)
    driver = random.choice(driver_pool)
    pickup = rand_point(city)
    dropoff = rand_point(city)
    distance_km = money(random.uniform(1.5, 38.0))
    duration_min = money(float(distance_km) * random.uniform(2.0, 4.5) + random.uniform(4.0, 12.0))
    accept_delay = random.uniform(0.4, 5.5) + (2.5 if context["is_peak"] else 0) + (2.5 if context["rain"] else 0) + (1.8 if context["supply_shortage"] else 0)
    arrive_delay = random.uniform(2.0, 16.0) + (4.0 if context["rain"] else 0) + (2.0 if context["supply_shortage"] else 0)
    pickup_wait = random.uniform(0.5, 7.0)
    ride_duration = max(5.0, float(duration_min) * random.uniform(0.75, 1.35))

    preliminary_fare = fare_breakdown(distance_km, duration_min, service_type, context)
    outcome = choose_outcome(context, accept_delay, arrive_delay, preliminary_fare["surge_multiplier"])

    accepted_at = None
    arrived_at = None
    started_at = None
    completed_at = None
    cancelled_at = None
    cancelled_by_type = None
    cancel_reason_code = None
    final_status = "REQUESTED"
    driver_id = None
    vehicle_id = None

    add_status(batch, ride_id, None, "REQUESTED", "RIDER", rider_id, requested_at)
    batch.pg_location_rows.append((ride_id, "PICKUP_REQUESTED", pickup[0], pickup[1], fake.street_address(), pg_dt(requested_at), pg_dt(requested_at)))
    batch.pg_location_rows.append((ride_id, "DROPOFF_REQUESTED", dropoff[0], dropoff[1], fake.street_address(), pg_dt(requested_at), pg_dt(requested_at)))

    if outcome == "RIDER_CANCEL_BEFORE_ACCEPT":
        cancelled_at = requested_at + timedelta(minutes=random.uniform(1.0, min(5.0, accept_delay + 0.5)))
        final_status = "CANCELLED"
        cancelled_by_type = "RIDER"
        cancel_reason_code = "RIDER_CHANGED_MIND"
        add_status(batch, ride_id, "REQUESTED", "CANCELLED", "RIDER", rider_id, cancelled_at, cancel_reason_code)
    else:
        accepted_at = requested_at + timedelta(minutes=accept_delay)
        driver_id = driver.driver_id
        vehicle_id = driver.vehicle_id
        add_status(batch, ride_id, "REQUESTED", "ACCEPTED", "DRIVER", driver.driver_id, accepted_at)

        if outcome in {"RIDER_CANCEL_AFTER_ACCEPT", "DRIVER_CANCEL"}:
            cancel_wait = random.uniform(1.0, 8.0)
            cancelled_at = accepted_at + timedelta(minutes=cancel_wait)
            final_status = "CANCELLED"
            cancel_reason_code = "RIDER_NO_LONGER_NEEDED" if outcome == "RIDER_CANCEL_AFTER_ACCEPT" else "DRIVER_VEHICLE_ISSUE"
            cancelled_by_type = "RIDER" if outcome == "RIDER_CANCEL_AFTER_ACCEPT" else "DRIVER"
            who_id = rider_id if cancelled_by_type == "RIDER" else driver.driver_id
            add_status(batch, ride_id, "ACCEPTED", "CANCELLED", cancelled_by_type, who_id, cancelled_at, cancel_reason_code)
        else:
            arrived_at = accepted_at + timedelta(minutes=arrive_delay)
            started_at = arrived_at + timedelta(minutes=pickup_wait)
            completed_at = started_at + timedelta(minutes=ride_duration)
            final_status = "PAYMENT_FAILED" if outcome == "PAYMENT_FAILED" else "COMPLETED"
            add_status(batch, ride_id, "ACCEPTED", "ARRIVED", "DRIVER", driver.driver_id, arrived_at)
            add_status(batch, ride_id, "ARRIVED", "IN_PROGRESS", "DRIVER", driver.driver_id, started_at)
            add_status(batch, ride_id, "IN_PROGRESS", final_status, "SYSTEM", None, completed_at)
            batch.pg_location_rows.append((ride_id, "PICKUP_ACTUAL", pickup[0], pickup[1], fake.street_address(), pg_dt(arrived_at), pg_dt(arrived_at)))
            batch.pg_location_rows.append((ride_id, "DROPOFF_ACTUAL", dropoff[0], dropoff[1], fake.street_address(), pg_dt(completed_at), pg_dt(completed_at)))

            # Tracking points: intentionally bounded for large historical datasets.
            tracking_points = min(HIST_TRACKING_POINTS_MAX, max(2, int(ride_duration // 12)))
            for idx in range(tracking_points):
                recorded_at = started_at + timedelta(minutes=ride_duration * ((idx + 1) / tracking_points))
                ratio = (idx + 1) / tracking_points
                lat = pickup[0] + (dropoff[0] - pickup[0]) * ratio + random.uniform(-0.001, 0.001)
                lon = pickup[1] + (dropoff[1] - pickup[1]) * ratio + random.uniform(-0.001, 0.001)
                batch.pg_tracking_rows.append((ride_id, driver.driver_id, round(lat, 6), round(lon, 6), money(random.uniform(8, 55)), pg_dt(recorded_at), pg_dt(recorded_at)))

            discount, promo = apply_promo_for_historical(rider_id, preliminary_fare["total_fare"], context, completed_at, city, service_type, promos)
            fare = fare_breakdown(distance_km, duration_min, service_type, context, discount)
            batch.pg_fare_rows.append((
                ride_id,
                distance_km,
                duration_min,
                fare["base_fare"],
                fare["distance_fare"],
                fare["time_fare"],
                fare["surge_multiplier"],
                fare["surge_amount"],
                fare["discount_amount"],
                fare["tax_amount"],
                fare["platform_fee"],
                fare["driver_earning"],
                fare["total_fare"],
                f"{city}_{service_type}_V1",
                pg_dt(completed_at),
                pg_dt(completed_at),
                pg_dt(completed_at),
            ))

            if promo and discount > 0:
                batch.my_promo_rows.append((promo.promotion_id, ride_id, rider_id, discount, mysql_dt(completed_at), mysql_dt(completed_at), mysql_dt(completed_at)))

            payment_method = choose_payment_method(rider_id, payment_methods)
            payment_status, failure_code = payment_status_for(payment_method, outcome, context)
            payment_at = completed_at + timedelta(minutes=random.uniform(0.2, 4.0))
            payment_transaction_id = payment_id_counter[0]
            payment_id_counter[0] += 1
            batch.my_payment_rows.append((
                payment_transaction_id,
                ride_id,
                rider_id,
                payment_method.payment_method_id or None,
                payment_method.provider_name,
                f"pay_{uuid.uuid4().hex}" if payment_status == "PAID" else None,
                f"ride-{ride_id}-{uuid.uuid4().hex}",
                fare["total_fare"],
                money(fare["total_fare"] * Decimal("0.01")),
                payment_status,
                failure_code,
                f"Demo {failure_code}" if failure_code else None,
                mysql_dt(payment_at if payment_status == "PAID" else None),
                mysql_dt(payment_at if payment_status == "PAID" else None),
                mysql_dt(payment_at if payment_status == "PAID" else None),
                mysql_dt(payment_at),
                mysql_dt(payment_at),
            ))

            if payment_status == "PAID" and random.random() < 0.10:
                rating = choose_rating(context, accept_delay, arrive_delay, fare["surge_multiplier"])
                review_at = completed_at + timedelta(minutes=random.uniform(2, 180))
                review_deleted = random.random() < 0.012
                batch.my_review_rows.append((
                    ride_id,
                    rider_id,
                    driver.driver_id,
                    rating,
                    random.choice(["Good trip", "Clean vehicle", "Driver late", "Smooth ride", "Pickup was difficult", None]),
                    "DELETED" if review_deleted else "PUBLISHED",
                    mysql_dt(review_at),
                    mysql_dt(review_at + timedelta(hours=1) if review_deleted else review_at),
                    mysql_dt(review_at + timedelta(hours=1)) if review_deleted else None,
                ))
                if rating <= 2:
                    ticket_at = review_at + timedelta(minutes=random.uniform(1, 30))
                    resolved = ticket_at + timedelta(hours=random.uniform(1, 48)) if random.random() < 0.72 else None
                    priority = "HIGH" if rating == 1 or context["rain"] else random.choice(["MEDIUM", "HIGH"])
                    category = random.choice(["LOW_RATING_COMPLAINT", "DRIVER_BEHAVIOR", "LATE_ARRIVAL", "PRICE_DISPUTE"])
                    batch.my_ticket_rows.append((ride_id, rider_id, driver.driver_id, category, "RESOLVED" if resolved else "OPEN", priority, mysql_dt(ticket_at), mysql_dt(resolved), mysql_dt(ticket_at), mysql_dt(resolved or ticket_at)))
                    if random.random() < 0.42:
                        refund_amt = money(fare["total_fare"] * Decimal(str(random.choice([0.25, 0.5, 1.0]))))
                        refund_requested = ticket_at + timedelta(minutes=random.uniform(5, 120))
                        refund_processed = refund_requested + timedelta(minutes=random.uniform(10, 240))
                        batch.my_refund_rows.append((payment_transaction_id, ride_id, refund_amt, "SERVICE_QUALITY", "PROCESSED", mysql_dt(refund_requested), mysql_dt(refund_processed), mysql_dt(refund_requested), mysql_dt(refund_processed)))

    updated_at = cancelled_at or completed_at or accepted_at or requested_at
    batch.pg_ride_rows.append((
        ride_id,
        rider_id,
        driver_id,
        vehicle_id,
        final_status,
        service_type,
        city,
        pg_dt(requested_at),
        pg_dt(accepted_at) if accepted_at else None,
        pg_dt(arrived_at) if arrived_at else None,
        pg_dt(started_at) if started_at else None,
        pg_dt(completed_at) if completed_at else None,
        pg_dt(cancelled_at) if cancelled_at else None,
        cancelled_by_type,
        cancel_reason_code,
        distance_km,
        duration_min,
        pg_dt(requested_at),
        pg_dt(updated_at),
    ))


def generate_historical() -> None:
    if HIST_END_DATE < HIST_START_DATE:
        raise ValueError("HIST_END_DATE must be >= HIST_START_DATE")

    random.seed(HIST_RANDOM_SEED)
    Faker.seed(HIST_RANDOM_SEED)

    if HIST_TRUNCATE_BEFORE_LOAD:
        truncate_all_sources()

    seed_sources(datetime.combine(HIST_START_DATE, dt_time(0, 0, tzinfo=JKT)) - timedelta(days=90))

    riders_by_city = load_riders_by_city()
    drivers_by_city_service = load_drivers_by_city_service()
    payment_methods = load_payment_methods_by_rider()
    promos = load_promos()
    payment_id_counter = [next_payment_transaction_id_start()]

    logger.info(
        "Historical generation started start=%s end=%s base_rides_per_day=%s batch_size=%s riders=%s driver_pools=%s promos=%s",
        HIST_START_DATE,
        HIST_END_DATE,
        HIST_BASE_RIDES_PER_DAY,
        HIST_BATCH_SIZE,
        len(riders_by_city.get("ALL", [])),
        len(drivers_by_city_service),
        len(promos),
    )

    batch = HistoricalBatch()
    total_generated = 0
    total_days = (HIST_END_DATE - HIST_START_DATE).days + 1
    day_no = 0

    with pg_conn() as pg, my_conn() as my:
        for d in daterange(HIST_START_DATE, HIST_END_DATE):
            day_no += 1
            day_target = rides_per_day(d)
            day_generated = 0
            add_driver_shifts(batch, d, drivers_by_city_service)

            for hour in range(24):
                n = rides_per_hour(day_target, d, hour)
                if n <= 0:
                    continue
                ride_ids = reserve_ride_ids(pg, n)
                for ride_id in ride_ids:
                    requested_at = random_datetime_in_hour(d, hour)
                    generate_historical_ride(
                        batch,
                        ride_id,
                        requested_at,
                        riders_by_city,
                        drivers_by_city_service,
                        payment_methods,
                        promos,
                        payment_id_counter,
                    )
                    day_generated += 1
                    total_generated += 1
                    if batch.ride_count >= HIST_BATCH_SIZE:
                        insert_batch(pg, my, batch)
                        pg.commit()
                        my.commit()
                        batch.clear()

            if day_no % max(1, HIST_COMMIT_EVERY_DAYS) == 0 and batch.ride_count > 0:
                insert_batch(pg, my, batch)
                pg.commit()
                my.commit()
                batch.clear()

            if day_no == 1 or day_no % HIST_PROGRESS_EVERY_DAYS == 0 or day_no == total_days:
                logger.info("Historical progress day=%s/%s date=%s generated_today=%s total_generated=%s", day_no, total_days, d, day_generated, total_generated)

        if batch.ride_count > 0:
            insert_batch(pg, my, batch)
            pg.commit()
            my.commit()
            batch.clear()

    logger.info("Historical generation completed total_rides=%s start=%s end=%s", total_generated, HIST_START_DATE, HIST_END_DATE)


# =========================================================
# Realtime compatibility mode
# =========================================================

def run_realtime() -> None:
    """Keep a lightweight realtime mode. This delegates to a simplified historical ride writer with current timestamps."""
    random.seed()
    seed_sources(datetime.now(JKT) - timedelta(days=90))
    riders_by_city = load_riders_by_city()
    drivers_by_city_service = load_drivers_by_city_service()
    payment_methods = load_payment_methods_by_rider()
    promos = load_promos()
    payment_id_counter = [next_payment_transaction_id_start()]
    clock = SimClock(SIM_START_AT, SIM_SECONDS_PER_MINUTE)
    spawn_interval_sim_min = 1.0 / max(RIDES_PER_MINUTE, 0.1)
    logger.info("Realtime generation started mode=%s sim_start=%s", GENERATOR_MODE, clock.sim_start.isoformat())

    with pg_conn() as pg, my_conn() as my:
        while True:
            batch = HistoricalBatch()
            ride_id = reserve_ride_ids(pg, 1)[0]
            generate_historical_ride(batch, ride_id, clock.now(), riders_by_city, drivers_by_city_service, payment_methods, promos, payment_id_counter)
            insert_batch(pg, my, batch)
            pg.commit()
            my.commit()
            clock.sleep_sim_minutes(spawn_interval_sim_min)


# =========================================================
# Validation helper
# =========================================================

def log_validation_summary() -> None:
    with pg_conn() as pg:
        ride_count = fetch_one_pg(pg, "select count(*) from ride")
        min_date = fetch_one_pg(pg, "select min(date(requested_at at time zone 'Asia/Jakarta')) from ride")
        max_date = fetch_one_pg(pg, "select max(date(requested_at at time zone 'Asia/Jakarta')) from ride")
        status_rows = fetch_all_pg(pg, "select ride_status, count(*) from ride group by 1 order by 2 desc")
    with my_conn() as my:
        payment_rows = fetch_all_mysql(my, "select payment_status, count(*) from payment_transaction group by 1 order by 2 desc")
        promo_count = fetch_all_mysql(my, "select count(*) from promo_usage")
    logger.info("Validation rides=%s min_date=%s max_date=%s statuses=%s payments=%s promo_usage=%s", ride_count, min_date, max_date, status_rows, payment_rows, promo_count[0][0] if promo_count else 0)


def main() -> None:
    logger.info("Generator starting mode=%s", GENERATOR_MODE)
    if GENERATOR_MODE in {"historical", "history", "batch"}:
        generate_historical()
        log_validation_summary()
    elif GENERATOR_MODE in {"realtime", "portfolio", "live"}:
        run_realtime()
    else:
        raise ValueError(f"Unknown GENERATOR_MODE={GENERATOR_MODE!r}; use historical or realtime")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Generator stopped")
