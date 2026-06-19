import logging
import os
import random
import string
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

import psycopg
import pymysql
from faker import Faker

fake = Faker("id_ID")
JKT = timezone(timedelta(hours=7))

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@postgres-ops:5432/ride_ops_pg")
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql-billing")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "billing_growth_db")
MYSQL_USER = os.getenv("MYSQL_USER", "ride_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "ride_pass")

GENERATOR_MODE = os.getenv("GENERATOR_MODE", "portfolio")
SIM_START_AT = os.getenv("SIM_START_AT", "2024-01-01T00:00:00+07:00")
SIM_SECONDS_PER_MINUTE = os.getenv("SIM_SECONDS_PER_MINUTE", "0.05")
RIDES_PER_MINUTE = os.getenv("RIDES_PER_MINUTE", "10")
MAX_CONCURRENT_RIDES = os.getenv("MAX_CONCURRENT_RIDES", "50")
SEED_RIDERS = os.getenv("SEED_RIDERS", "5000")
SEED_DRIVERS = os.getenv("SEED_DRIVERS", "1500")
SEED_PROMOTIONS = os.getenv("SEED_PROMOTIONS", "30")
ENABLE_MAINTENANCE_EVENTS = os.getenv("ENABLE_MAINTENANCE_EVENTS", "true").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("portfolio-generator")

CITY_BOX = {
    "JKT": (-6.35, -6.08, 106.65, 106.95),
    "BDG": (-6.98, -6.82, 107.55, 107.75),
    "SBY": (-7.35, -7.20, 112.65, 112.85),
}
CITY_WEIGHTS = {"JKT": 0.62, "BDG": 0.20, "SBY": 0.18}

@dataclass
class Driver:
    driver_id: int
    vehicle_id: int
    vehicle_type: str
    city_code: str

class SimClock:
    def __init__(self, start_at: str, sim_seconds_per_minute: float):
        dt = datetime.fromisoformat(start_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JKT)
        self.sim_start = dt.astimezone(JKT)
        self.real_start = datetime.now(JKT)
        self.sim_seconds_per_minute = sim_seconds_per_minute

    def now(self) -> datetime:
        real_elapsed = (datetime.now(JKT) - self.real_start).total_seconds()
        sim_minutes = real_elapsed / self.sim_seconds_per_minute
        return self.sim_start + timedelta(minutes=sim_minutes)

    def sleep_sim_minutes(self, minutes: float):
        time.sleep(max(0.02, minutes * self.sim_seconds_per_minute))

def money(value) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def mysql_dt(dt: datetime):
    if not dt:
        return None
    return dt.astimezone(JKT).replace(tzinfo=None)

def plate() -> str:
    digits = random.randint(1000, 9999)
    suffix = "".join(random.choices(string.ascii_uppercase, k=random.choice([2, 3])))
    return f"B {digits} {suffix}"

def pg_conn():
    return psycopg.connect(POSTGRES_DSN)

def my_conn():
    return pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE, autocommit=False)

def fetch_one_pg(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        return row[0] if row else None

def fetch_all_pg(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()

def fetch_all_mysql(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()

def choose_city(now: datetime) -> str:
    # Jakarta becomes more dominant during rush hour.
    hour = now.hour
    weights = CITY_WEIGHTS.copy()
    if hour in range(7, 10) or hour in range(17, 21):
        weights["JKT"] += 0.10
        weights["BDG"] -= 0.04
        weights["SBY"] -= 0.06
    cities, probs = zip(*weights.items())
    return random.choices(cities, weights=probs, k=1)[0]

def choose_service(now: datetime, city: str) -> str:
    hour = now.hour
    is_weekend = now.weekday() >= 5
    base = {"BIKE": 0.58, "CAR": 0.34, "XL": 0.08}
    if hour in range(7, 10):
        base["BIKE"] += 0.12; base["CAR"] -= 0.08; base["XL"] -= 0.04
    if is_weekend:
        base["BIKE"] -= 0.10; base["CAR"] += 0.06; base["XL"] += 0.04
    if city == "JKT" and hour in range(17, 21):
        base["BIKE"] += 0.06; base["CAR"] -= 0.02; base["XL"] -= 0.04
    keys, vals = zip(*base.items())
    return random.choices(keys, weights=vals, k=1)[0]

def business_context(now: datetime, city: str):
    hour = now.hour
    is_peak = hour in range(7, 10) or hour in range(17, 21)
    is_weekend = now.weekday() >= 5
    rain = city == "JKT" and (hour in range(16, 19)) and random.random() < 0.28
    payment_incident = city == "SBY" and hour in range(19, 21) and random.random() < 0.25
    promo_boost = now.day % 10 in (1, 2, 3)
    return {
        "is_peak": is_peak,
        "is_weekend": is_weekend,
        "rain": rain,
        "payment_incident": payment_incident,
        "promo_boost": promo_boost,
    }

def rand_point(city):
    lat_min, lat_max, lon_min, lon_max = CITY_BOX[city]
    return round(random.uniform(lat_min, lat_max), 6), round(random.uniform(lon_min, lon_max), 6)

def seed_sources(clock: SimClock):
    with pg_conn() as pg, my_conn() as my:
        existing = fetch_one_pg(pg, "select count(*) from rider_account")
        if existing and existing >= SEED_RIDERS:
            logger.info("Seed already exists: riders=%s", existing)
            return
        base = clock.now() - timedelta(days=90)
        logger.info("Seeding source systems: riders=%s drivers=%s promotions=%s", SEED_RIDERS, SEED_DRIVERS, SEED_PROMOTIONS)
        with pg.cursor() as cur:
            for i in range(SEED_RIDERS):
                city = random.choices(list(CITY_WEIGHTS), weights=list(CITY_WEIGHTS.values()), k=1)[0]
                created = base + timedelta(minutes=i * random.randint(2, 8))
                cur.execute(
                    """
                    insert into rider_account(username, full_name, email, phone_number, account_status, city_code, created_at, updated_at)
                    values(%s,%s,%s,%s,'ACTIVE',%s,%s,%s)
                    on conflict(username) do nothing
                    """,
                    (f"rider_{i+1:05d}", fake.name(), f"rider_{i+1:05d}@example.test", f"+62813{10000000+i:08d}", city, created, created)
                )
            makes = {
                "BIKE": [("Yamaha", "NMAX"), ("Honda", "Vario"), ("Honda", "Beat")],
                "CAR": [("Toyota", "Avanza"), ("Honda", "Brio"), ("Daihatsu", "Xenia")],
                "XL": [("Toyota", "Innova"), ("Toyota", "Voxy")]
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
                    (fake.name(), f"+62817{10000000+i:08d}", city, money(random.uniform(4.55, 5.00)), random.randint(10, 500), created, created)
                )
                driver_id = cur.fetchone()[0]
                make, model = random.choice(makes[vtype])
                cur.execute(
                    """
                    insert into vehicle(driver_id, license_plate, vehicle_type, vehicle_make, vehicle_model, vehicle_year, vehicle_status, created_at, updated_at)
                    values(%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s) returning vehicle_id
                    """,
                    (driver_id, plate(), vtype, make, model, random.randint(2016, 2025), created, created)
                )
                vehicle_id = cur.fetchone()[0]
                cur.execute(
                    """insert into driver_vehicle_assignment(driver_id, vehicle_id, assigned_from, is_active, created_at, updated_at)
                    values(%s,%s,%s,true,%s,%s)""",
                    (driver_id, vehicle_id, created, created, created)
                )
        pg.commit()
        with my.cursor() as cur:
            for rider_id in range(1, SEED_RIDERS + 1):
                n_methods = random.choice([1, 1, 2])
                for j in range(n_methods):
                    method = random.choice(["EWALLET", "CARD", "BANK_TRANSFER", "CASH"])
                    provider = {"EWALLET": "demo_ewallet", "CARD": "demo_card", "BANK_TRANSFER": "demo_bank", "CASH": "cash"}[method]
                    cur.execute(
                        """insert into payment_method(rider_id, method_code, provider_name, masked_account, payment_method_status, is_default, created_at, updated_at)
                        values(%s,%s,%s,%s,'ACTIVE',%s,%s,%s)""",
                        (rider_id, method, provider, "****" + str(random.randint(1000,9999)), j == 0, mysql_dt(base), mysql_dt(base))
                    )
            for i in range(SEED_PROMOTIONS):
                valid_from = mysql_dt(clock.now() - timedelta(days=30))
                valid_to = mysql_dt(clock.now() + timedelta(days=730))
                dtype = random.choice(["PERCENT", "FIXED"])
                cur.execute(
                    """insert into promotion(promo_code, promo_description, discount_type, discount_pct, discount_amount, max_discount_amount, min_fare_amount, valid_from, valid_to, promotion_status, created_at, updated_at)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s)""",
                    (
                        f"PORTO{i+1:02d}", f"Portfolio promo {i+1}", dtype,
                        money(random.choice([5, 10, 15, 20])) if dtype == "PERCENT" else None,
                        money(random.choice([5000, 10000, 15000])) if dtype == "FIXED" else None,
                        money(random.choice([12000, 20000, 25000])), money(random.choice([20000, 30000, 40000])),
                        valid_from, valid_to, mysql_dt(base), mysql_dt(base)
                    )
                )
        my.commit()
        logger.info("Seed completed")

def load_riders(city=None):
    with pg_conn() as conn:
        if city:
            rows = fetch_all_pg(conn, "select rider_id from rider_account where account_status='ACTIVE' and city_code=%s and deleted_at is null", (city,))
        else:
            rows = fetch_all_pg(conn, "select rider_id from rider_account where account_status='ACTIVE' and deleted_at is null")
        return [r[0] for r in rows]

def load_drivers(city, service_type):
    with pg_conn() as conn:
        rows = fetch_all_pg(conn, """
            select d.driver_id, v.vehicle_id, v.vehicle_type, d.city_code
            from driver_profile d
            join vehicle v on v.driver_id = d.driver_id and v.vehicle_status='ACTIVE' and v.deleted_at is null
            where d.city_code=%s and d.driver_status in ('AVAILABLE','OFFLINE') and d.verification_status='VERIFIED' and d.deleted_at is null
              and v.vehicle_type=%s
        """, (city, service_type))
        return [Driver(*r) for r in rows]

def load_promos(now):
    with my_conn() as conn:
        rows = fetch_all_mysql(conn, """
            select promotion_id, promo_code, discount_type, coalesce(discount_pct,0), coalesce(discount_amount,0), coalesce(max_discount_amount,0), coalesce(min_fare_amount,0)
            from promotion
            where promotion_status='ACTIVE' and valid_from <= %s and valid_to >= %s and deleted_at is null
        """, (mysql_dt(now), mysql_dt(now)))
        return rows

def insert_status(pg, ride_id, old_status, new_status, who_type, who_id, changed_at, reason=None, note=None):
    with pg.cursor() as cur:
        cur.execute(
            """insert into ride_status_history(ride_id, old_status, new_status, changed_by_type, changed_by_id, reason_code, reason_note, changed_at, created_at)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (ride_id, old_status, new_status, who_type, who_id, reason, note, changed_at, changed_at)
        )

def fare_breakdown(distance_km, duration_min, service_type, context, discount=Decimal("0.00")):
    if service_type == "BIKE": base, per_km, per_min = 7000, 2400, 250
    elif service_type == "XL": base, per_km, per_min = 15000, 5200, 600
    else: base, per_km, per_min = 10000, 3800, 450
    surge_choices = [1.0, 1.0, 1.0, 1.1, 1.2]
    if context["is_peak"]: surge_choices += [1.25, 1.35]
    if context["rain"]: surge_choices += [1.35, 1.5]
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
        "base_fare": base_fare, "distance_fare": distance_fare, "time_fare": time_fare,
        "surge_multiplier": surge_multiplier, "surge_amount": surge_amount, "discount_amount": discount,
        "tax_amount": tax_amount, "platform_fee": platform_fee, "driver_earning": driver_earning, "total_fare": total_fare,
    }

def apply_promo(ride_id, rider_id, preliminary_fare, context, completed_at):
    promos = load_promos(completed_at)
    if not promos:
        return Decimal("0.00"), None
    chance = 0.18 + (0.18 if context["promo_boost"] else 0)
    if random.random() > chance:
        return Decimal("0.00"), None
    promo = random.choice(promos)
    promotion_id, promo_code, dtype, pct, fixed, max_discount, min_fare = promo
    if preliminary_fare < Decimal(str(min_fare)):
        return Decimal("0.00"), None
    if dtype == "PERCENT":
        discount = min(money(preliminary_fare * Decimal(str(pct)) / Decimal("100")), Decimal(str(max_discount)))
    else:
        discount = min(Decimal(str(fixed)), Decimal(str(max_discount)))
    return money(discount), promo

def create_payment_and_growth(ride_id, rider_id, driver_id, amount, status_hint, context, fare, completed_at):
    with my_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select payment_method_id, method_code, provider_name from payment_method where rider_id=%s and payment_method_status='ACTIVE' order by is_default desc, payment_method_id limit 1", (rider_id,))
            pm = cur.fetchone()
            payment_method_id, method_code, provider_name = pm if pm else (None, "CASH", "cash")
            incident_failed = context["payment_incident"] and method_code in ("EWALLET", "CARD")
            status = "FAILED" if status_hint == "PAYMENT_FAILED" or incident_failed else random.choices(["PAID", "FAILED"], weights=[96,4], k=1)[0]
            failure_code = None
            if status == "FAILED":
                failure_code = random.choice(["INSUFFICIENT_BALANCE", "GATEWAY_TIMEOUT", "CARD_DECLINED", "EWALLET_PROVIDER_DOWN"] if incident_failed else ["INSUFFICIENT_BALANCE", "GATEWAY_TIMEOUT", "CARD_DECLINED"])
            payment_at = completed_at + timedelta(minutes=random.uniform(0.2, 4.0))
            cur.execute(
                """insert into payment_transaction(ride_id, rider_id, payment_method_id, provider_name, provider_transaction_id, idempotency_key, amount, method_fee, payment_status, failure_code, failure_message, authorized_at, captured_at, paid_at, created_at, updated_at)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    ride_id, rider_id, payment_method_id, provider_name,
                    f"pay_{uuid.uuid4().hex}" if status == "PAID" else None,
                    f"ride-{ride_id}-{uuid.uuid4().hex}", amount, money(amount * Decimal("0.01")), status, failure_code,
                    f"Demo {failure_code}" if failure_code else None,
                    mysql_dt(payment_at if status == "PAID" else None), mysql_dt(payment_at if status == "PAID" else None), mysql_dt(payment_at if status == "PAID" else None),
                    mysql_dt(payment_at), mysql_dt(payment_at)
                )
            )
            payment_transaction_id = cur.lastrowid
            if status == "PAID" and random.random() < 0.10:
                rating = random.choices([5,4,3,2,1], weights=[70,20,7,2,1], k=1)[0]
                review_at = completed_at + timedelta(minutes=random.uniform(2, 180))
                cur.execute(
                    """insert into review(ride_id, reviewer_type, reviewer_id, reviewee_type, reviewee_id, rating_score, comments, review_status, created_at, updated_at)
                    values(%s,'RIDER',%s,'DRIVER',%s,%s,%s,'PUBLISHED',%s,%s)""",
                    (ride_id, rider_id, driver_id, rating, random.choice(["Good trip", "Clean vehicle", "Driver late", "Smooth ride", None]), mysql_dt(review_at), mysql_dt(review_at))
                )
                if rating <= 2:
                    cur.execute(
                        """insert into support_ticket(ride_id, rider_id, driver_id, ticket_category, ticket_status, priority, opened_at, created_at, updated_at)
                        values(%s,%s,%s,'LOW_RATING_COMPLAINT','OPEN','HIGH',%s,%s,%s)""",
                        (ride_id, rider_id, driver_id, mysql_dt(review_at), mysql_dt(review_at), mysql_dt(review_at))
                    )
                    if random.random() < 0.45:
                        refund_amt = money(amount * Decimal(str(random.choice([0.25, 0.5, 1.0]))))
                        cur.execute(
                            """insert into payment_refund(payment_transaction_id, ride_id, refund_amount, refund_reason_code, refund_status, requested_at, processed_at, created_at, updated_at)
                            values(%s,%s,%s,'SERVICE_QUALITY','PROCESSED',%s,%s,%s,%s)""",
                            (payment_transaction_id, ride_id, refund_amt, mysql_dt(review_at), mysql_dt(review_at + timedelta(minutes=20)), mysql_dt(review_at), mysql_dt(review_at + timedelta(minutes=20)))
                        )
            if random.random() < 0.015:
                # Rare soft-delete review to demonstrate delete-like data changes safely.
                cur.execute("update review set review_status='DELETED', deleted_at=%s, updated_at=%s where ride_id=%s order by review_id desc limit 1", (mysql_dt(completed_at + timedelta(hours=1)), mysql_dt(completed_at + timedelta(hours=1)), ride_id))
        conn.commit()
        return status

def maybe_driver_maintenance(now):
    if not ENABLE_MAINTENANCE_EVENTS or random.random() > 0.03:
        return
    with pg_conn() as conn:
        rows = fetch_all_pg(conn, "select driver_id from driver_profile where deleted_at is null order by random() limit 1")
        if not rows:
            return
        driver_id = rows[0][0]
        status = random.choice(["OFFLINE", "AVAILABLE", "SUSPENDED"])
        with conn.cursor() as cur:
            cur.execute("update driver_profile set driver_status=%s, updated_at=%s where driver_id=%s", (status, now, driver_id))
            cur.execute("insert into driver_shift(driver_id, shift_status, started_at, ended_at, created_at, updated_at) values(%s,%s,%s,%s,%s,%s)", (driver_id, status, now, None if status != "OFFLINE" else now + timedelta(hours=2), now, now))
        conn.commit()
        logger.info("Driver maintenance event driver=%s status=%s", driver_id, status)

def generate_one_ride(clock: SimClock):
    now = clock.now()
    city = choose_city(now)
    service_type = choose_service(now, city)
    context = business_context(now, city)
    riders = load_riders(city) or load_riders()
    drivers = load_drivers(city, service_type)
    if not riders or not drivers:
        logger.warning("Missing pools city=%s service=%s riders=%s drivers=%s", city, service_type, len(riders), len(drivers))
        return
    rider_id = random.choice(riders)
    driver = random.choice(drivers)
    requested_at = now
    pickup = rand_point(city); dropoff = rand_point(city)
    distance_km = money(random.uniform(1.5, 38.0))
    duration_min = money(float(distance_km) * random.uniform(2.0, 4.5) + random.uniform(4.0, 12.0))
    accept_delay = random.uniform(0.5, 5.5) + (2.5 if context["is_peak"] else 0) + (2.5 if context["rain"] else 0)
    arrive_delay = random.uniform(2.0, 16.0) + (4.0 if context["rain"] else 0)
    pickup_wait = random.uniform(0.5, 7.0)
    ride_duration = max(5.0, float(duration_min) * random.uniform(0.75, 1.35))

    weights = {"COMPLETED": 82, "RIDER_CANCEL_BEFORE_ACCEPT": 4, "RIDER_CANCEL_AFTER_ACCEPT": 5, "DRIVER_CANCEL": 4, "PAYMENT_FAILED": 5}
    if context["rain"]: weights["COMPLETED"] -= 8; weights["RIDER_CANCEL_AFTER_ACCEPT"] += 4; weights["DRIVER_CANCEL"] += 4
    if context["payment_incident"]: weights["PAYMENT_FAILED"] += 10; weights["COMPLETED"] -= 10
    outcome = random.choices(list(weights), weights=list(weights.values()), k=1)[0]

    with pg_conn() as pg:
        with pg.cursor() as cur:
            cur.execute(
                """insert into ride(rider_id, driver_id, vehicle_id, ride_status, service_type, city_code, requested_at, estimated_distance_km, estimated_duration_min, created_at, updated_at)
                values(%s, null, null, 'REQUESTED', %s, %s, %s, %s, %s, %s, %s) returning ride_id""",
                (rider_id, service_type, city, requested_at, distance_km, duration_min, requested_at, requested_at)
            )
            ride_id = cur.fetchone()[0]
            insert_status(pg, ride_id, None, "REQUESTED", "RIDER", rider_id, requested_at)
            cur.execute(
                """insert into ride_location(ride_id, location_type, latitude, longitude, address_text, captured_at, created_at) values
                (%s,'PICKUP_REQUESTED',%s,%s,%s,%s,%s),
                (%s,'DROPOFF_REQUESTED',%s,%s,%s,%s,%s)""",
                (ride_id, pickup[0], pickup[1], fake.street_address(), requested_at, requested_at, ride_id, dropoff[0], dropoff[1], fake.street_address(), requested_at, requested_at)
            )
        pg.commit()

    clock.sleep_sim_minutes(accept_delay)
    if outcome == "RIDER_CANCEL_BEFORE_ACCEPT":
        cancelled_at = requested_at + timedelta(minutes=random.uniform(1.0, 5.0))
        with pg_conn() as pg:
            with pg.cursor() as cur:
                cur.execute("update ride set ride_status='CANCELLED', cancelled_at=%s, cancelled_by_type='RIDER', cancel_reason_code='RIDER_CHANGED_MIND', updated_at=%s where ride_id=%s", (cancelled_at, cancelled_at, ride_id))
                insert_status(pg, ride_id, "REQUESTED", "CANCELLED", "RIDER", rider_id, cancelled_at, "RIDER_CHANGED_MIND")
            pg.commit()
        return

    accepted_at = requested_at + timedelta(minutes=accept_delay)
    with pg_conn() as pg:
        with pg.cursor() as cur:
            cur.execute("update ride set ride_status='ACCEPTED', driver_id=%s, vehicle_id=%s, accepted_at=%s, updated_at=%s where ride_id=%s", (driver.driver_id, driver.vehicle_id, accepted_at, accepted_at, ride_id))
            cur.execute("update driver_profile set driver_status='ON_RIDE', updated_at=%s where driver_id=%s", (accepted_at, driver.driver_id))
            insert_status(pg, ride_id, "REQUESTED", "ACCEPTED", "DRIVER", driver.driver_id, accepted_at)
        pg.commit()

    if outcome in ("RIDER_CANCEL_AFTER_ACCEPT", "DRIVER_CANCEL"):
        cancel_wait = random.uniform(1.0, 8.0)
        clock.sleep_sim_minutes(cancel_wait)
        cancelled_at = accepted_at + timedelta(minutes=cancel_wait)
        reason = "RIDER_NO_LONGER_NEEDED" if outcome == "RIDER_CANCEL_AFTER_ACCEPT" else "DRIVER_VEHICLE_ISSUE"
        who = "RIDER" if outcome == "RIDER_CANCEL_AFTER_ACCEPT" else "DRIVER"
        who_id = rider_id if who == "RIDER" else driver.driver_id
        with pg_conn() as pg:
            with pg.cursor() as cur:
                cur.execute("update ride set ride_status='CANCELLED', cancelled_at=%s, cancelled_by_type=%s, cancel_reason_code=%s, updated_at=%s where ride_id=%s", (cancelled_at, who, reason, cancelled_at, ride_id))
                cur.execute("update driver_profile set driver_status='AVAILABLE', updated_at=%s where driver_id=%s", (cancelled_at, driver.driver_id))
                insert_status(pg, ride_id, "ACCEPTED", "CANCELLED", who, who_id, cancelled_at, reason)
            pg.commit()
        return

    clock.sleep_sim_minutes(arrive_delay)
    arrived_at = accepted_at + timedelta(minutes=arrive_delay)
    with pg_conn() as pg:
        with pg.cursor() as cur:
            cur.execute("update ride set ride_status='ARRIVED', arrived_at=%s, updated_at=%s where ride_id=%s", (arrived_at, arrived_at, ride_id))
            cur.execute("insert into ride_location(ride_id, location_type, latitude, longitude, address_text, captured_at, created_at) values(%s,'PICKUP_ACTUAL',%s,%s,%s,%s,%s)", (ride_id, pickup[0], pickup[1], fake.street_address(), arrived_at, arrived_at))
            insert_status(pg, ride_id, "ACCEPTED", "ARRIVED", "DRIVER", driver.driver_id, arrived_at)
        pg.commit()

    clock.sleep_sim_minutes(pickup_wait)
    started_at = arrived_at + timedelta(minutes=pickup_wait)
    with pg_conn() as pg:
        with pg.cursor() as cur:
            cur.execute("update ride set ride_status='IN_PROGRESS', started_at=%s, updated_at=%s where ride_id=%s", (started_at, started_at, ride_id))
            insert_status(pg, ride_id, "ARRIVED", "IN_PROGRESS", "DRIVER", driver.driver_id, started_at)
        pg.commit()

    points = min(12, max(2, int(ride_duration // 10)))
    for idx in range(points):
        recorded_at = started_at + timedelta(minutes=ride_duration * ((idx + 1) / points))
        lat = pickup[0] + (dropoff[0] - pickup[0]) * ((idx + 1) / points) + random.uniform(-0.001, 0.001)
        lon = pickup[1] + (dropoff[1] - pickup[1]) * ((idx + 1) / points) + random.uniform(-0.001, 0.001)
        with pg_conn() as pg:
            with pg.cursor() as cur:
                cur.execute("insert into ride_tracking_point(ride_id, driver_id, latitude, longitude, speed_kmh, recorded_at, created_at) values(%s,%s,%s,%s,%s,%s,%s)", (ride_id, driver.driver_id, round(lat,6), round(lon,6), money(random.uniform(8,55)), recorded_at, recorded_at))
            pg.commit()
        clock.sleep_sim_minutes(10)

    completed_at = started_at + timedelta(minutes=ride_duration)
    preliminary_fare = fare_breakdown(distance_km, duration_min, service_type, context)
    discount, promo = apply_promo(ride_id, rider_id, preliminary_fare["total_fare"], context, completed_at)
    fare = fare_breakdown(distance_km, duration_min, service_type, context, discount)
    final_status = "PAYMENT_FAILED" if outcome == "PAYMENT_FAILED" else "COMPLETED"

    with pg_conn() as pg:
        with pg.cursor() as cur:
            cur.execute("update ride set ride_status=%s, completed_at=%s, updated_at=%s where ride_id=%s", (final_status, completed_at, completed_at, ride_id))
            cur.execute("insert into ride_location(ride_id, location_type, latitude, longitude, address_text, captured_at, created_at) values(%s,'DROPOFF_ACTUAL',%s,%s,%s,%s,%s)", (ride_id, dropoff[0], dropoff[1], fake.street_address(), completed_at, completed_at))
            insert_status(pg, ride_id, "IN_PROGRESS", final_status, "SYSTEM", None, completed_at, None)
            cur.execute(
                """insert into ride_fare(ride_id, fare_type, fare_version, distance_km, duration_min, base_fare, distance_fare, time_fare, surge_multiplier, surge_amount, discount_amount, tax_amount, platform_fee, driver_earning, total_fare, fare_rule_code, calculated_at, created_at, updated_at)
                values(%s,'FINAL',1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (ride_id, distance_km, duration_min, fare["base_fare"], fare["distance_fare"], fare["time_fare"], fare["surge_multiplier"], fare["surge_amount"], fare["discount_amount"], fare["tax_amount"], fare["platform_fee"], fare["driver_earning"], fare["total_fare"], f"{city}_{service_type}_V1", completed_at, completed_at, completed_at)
            )
            cur.execute("update driver_profile set driver_status='AVAILABLE', rating_count=rating_count+1, updated_at=%s where driver_id=%s", (completed_at, driver.driver_id))
        pg.commit()

    if promo and discount > 0:
        with my_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("insert ignore into promo_usage(promotion_id, ride_id, rider_id, discount_amount_applied, used_at, created_at, updated_at) values(%s,%s,%s,%s,%s,%s,%s)", (promo[0], ride_id, rider_id, discount, mysql_dt(completed_at), mysql_dt(completed_at), mysql_dt(completed_at)))
            conn.commit()

    payment_status = create_payment_and_growth(ride_id, rider_id, driver.driver_id, fare["total_fare"], outcome, context, fare, completed_at)
    logger.info("ride_id=%s city=%s service=%s final=%s payment=%s fare=%s", ride_id, city, service_type, final_status, payment_status, fare["total_fare"])

def main():
    clock = SimClock(SIM_START_AT, SIM_SECONDS_PER_MINUTE)
    seed_sources(clock)
    logger.info("Generator started mode=%s sim_start=%s", GENERATOR_MODE, clock.sim_start.isoformat())
    spawn_interval_sim_min = 1.0 / max(RIDES_PER_MINUTE, 0.1)
    while True:
        maybe_driver_maintenance(clock.now())
        generate_one_ride(clock)
        clock.sleep_sim_minutes(spawn_interval_sim_min)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Generator stopped")
