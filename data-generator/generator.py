import asyncio
import logging
import os
import random
import string
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

from faker import Faker
from psycopg_pool import AsyncConnectionPool

fake = Faker("id_ID")
JKT = timezone(timedelta(hours=7))

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres-ops:5432/ride_ops_pg")
SEED_RIDERS = int(os.getenv("SEED_RIDERS", "120"))
SEED_DRIVERS = int(os.getenv("SEED_DRIVERS", "35"))
SEED_PROMOTIONS = int(os.getenv("SEED_PROMOTIONS", "8"))
RIDES_PER_MINUTE = float(os.getenv("RIDES_PER_MINUTE", "4"))
MAX_CONCURRENT_RIDES = int(os.getenv("MAX_CONCURRENT_RIDES", "12"))
SIM_SECONDS_PER_MINUTE = float(os.getenv("SIM_SECONDS_PER_MINUTE", "0.12"))
SIM_START_DAYS_AGO = int(os.getenv("SIM_START_DAYS_AGO", "3"))
TRACKING_INTERVAL_SIM_MINUTES = int(os.getenv("TRACKING_INTERVAL_SIM_MINUTES", "3"))
MAX_TRACKING_POINTS_PER_RIDE = int(os.getenv("MAX_TRACKING_POINTS_PER_RIDE", "24"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ride-generator")

# Rough bounding box around Jakarta.
LAT_MIN, LAT_MAX = -6.35, -6.08
LON_MIN, LON_MAX = 106.65, 106.95

@dataclass
class DriverAssignment:
    driver_id: int
    user_id: int
    vehicle_id: int
    vehicle_type: str

class SimClock:
    """Maps real elapsed time to simulated timestamps.

    The generator sleeps using scaled time, but timestamps in PostgreSQL
    represent realistic ride timelines: accepted minutes after request,
    completed tens of minutes later, payments after completion, and so on.
    """

    def __init__(self, start_days_ago: int, sim_seconds_per_minute: float):
        self.sim_start = datetime.now(JKT) - timedelta(days=start_days_ago)
        self.real_start = datetime.now(JKT)
        self.sim_seconds_per_minute = sim_seconds_per_minute

    def now(self) -> datetime:
        real_elapsed = (datetime.now(JKT) - self.real_start).total_seconds()
        sim_minutes = real_elapsed / self.sim_seconds_per_minute
        return self.sim_start + timedelta(minutes=sim_minutes)

    async def sleep_sim_minutes(self, minutes: float):
        await asyncio.sleep(max(0.02, minutes * self.sim_seconds_per_minute))


def money(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def plate() -> str:
    letters = "".join(random.choices(string.ascii_uppercase, k=random.choice([2, 3])))
    digits = random.randint(1000, 9999)
    suffix = "".join(random.choices(string.ascii_uppercase, k=random.choice([2, 3])))
    return f"B {digits} {suffix}"


def rand_point():
    return (round(random.uniform(LAT_MIN, LAT_MAX), 6), round(random.uniform(LON_MIN, LON_MAX), 6))


def interpolate(a, b, ratio):
    return round(a + (b - a) * ratio, 6)


async def fetch_scalar(conn, sql, params=None):
    cur = await conn.execute(sql, params or ())
    row = await cur.fetchone()
    return row[0] if row else None


async def seed_reference_data(pool: AsyncConnectionPool, clock: SimClock):
    async with pool.connection() as conn:
        existing_users = await fetch_scalar(conn, "SELECT count(*) FROM user_account")
        if existing_users and existing_users >= SEED_RIDERS + SEED_DRIVERS:
            logger.info("Seed data already exists: %s users", existing_users)
            return

        logger.info("Seeding riders=%s drivers=%s promotions=%s", SEED_RIDERS, SEED_DRIVERS, SEED_PROMOTIONS)
        base_time = clock.now() - timedelta(days=30)

        # Admin system user.
        admin_id = await fetch_scalar(
            conn,
            """
            INSERT INTO user_account
            (username,email,phone_number,password_hash,account_status,email_verified_at,phone_verified_at,last_login_at,created_at,updated_at)
            VALUES (%s,%s,%s,%s,'ACTIVE',%s,%s,%s,%s,%s)
            ON CONFLICT (username) DO UPDATE SET updated_at = EXCLUDED.updated_at
            RETURNING user_id
            """,
            ("admin_system", "admin@example.test", "+6280000000000", "demo_hash_not_for_prod", base_time, base_time, base_time, base_time, base_time),
        )

        role_ids = {}
        cur = await conn.execute("SELECT role_id, role_code FROM role")
        for row in await cur.fetchall():
            role_ids[row[1]] = row[0]

        rider_ids = []
        for i in range(SEED_RIDERS):
            created_at = base_time + timedelta(minutes=i * random.randint(3, 11))
            username = f"rider_{i+1:05d}"
            email = f"{username}@example.test"
            phone = f"+62813{10000000+i:08d}"
            user_id = await fetch_scalar(
                conn,
                """
                INSERT INTO user_account
                (username,email,phone_number,password_hash,account_status,email_verified_at,phone_verified_at,last_login_at,created_at,updated_at)
                VALUES (%s,%s,%s,%s,'ACTIVE',%s,%s,%s,%s,%s)
                ON CONFLICT (username) DO NOTHING
                RETURNING user_id
                """,
                (username, email, phone, "demo_hash_not_for_prod", created_at + timedelta(minutes=3), created_at + timedelta(minutes=4), created_at, created_at, created_at),
            )
            if user_id:
                rider_ids.append(user_id)
                await conn.execute(
                    """
                    INSERT INTO user_role(user_id,role_id,assigned_at,assigned_by,is_active)
                    VALUES (%s,%s,%s,%s,true)
                    ON CONFLICT DO NOTHING
                    """,
                    (user_id, role_ids["RIDER"], created_at, admin_id),
                )

        makes = [("Toyota","Avanza","CAR",6),("Honda","Brio","CAR",4),("Daihatsu","Xenia","CAR",6),("Yamaha","NMAX","BIKE",1),("Honda","Vario","BIKE",1),("Toyota","Innova","XL",7)]
        for i in range(SEED_DRIVERS):
            created_at = base_time + timedelta(minutes=i * random.randint(5, 17))
            username = f"driver_{i+1:05d}"
            user_id = await fetch_scalar(
                conn,
                """
                INSERT INTO user_account
                (username,email,phone_number,password_hash,account_status,email_verified_at,phone_verified_at,last_login_at,created_at,updated_at)
                VALUES (%s,%s,%s,%s,'ACTIVE',%s,%s,%s,%s,%s)
                ON CONFLICT (username) DO NOTHING
                RETURNING user_id
                """,
                (
                    username, f"{username}@example.test", f"+62817{10000000+i:08d}",
                    "demo_hash_not_for_prod", created_at + timedelta(minutes=3), created_at + timedelta(minutes=4),
                    created_at, created_at, created_at,
                ),
            )
            if not user_id:
                continue

            await conn.execute(
                """
                INSERT INTO user_role(user_id,role_id,assigned_at,assigned_by,is_active)
                VALUES (%s,%s,%s,%s,true)
                ON CONFLICT DO NOTHING
                """,
                (user_id, role_ids["DRIVER"], created_at, admin_id),
            )

            driver_id = await fetch_scalar(
                conn,
                """
                INSERT INTO driver_profile
                (user_id,license_number,license_expiry,driver_status,verification_status,verified_at,rating_avg,rating_count,created_at,updated_at)
                VALUES (%s,%s,%s,'AVAILABLE','VERIFIED',%s,%s,%s,%s,%s)
                RETURNING driver_id
                """,
                (
                    user_id, f"SIM-{i+1:06d}", (created_at + timedelta(days=365*3)).date(),
                    created_at + timedelta(days=1), money(random.uniform(4.65, 5.00)), random.randint(5, 280),
                    created_at, created_at,
                ),
            )

            for doc_type in ["KTP", "SIM", "STNK"]:
                await conn.execute(
                    """
                    INSERT INTO driver_document
                    (driver_id,document_type,document_number,document_file_url,verification_status,submitted_at,verified_at,verified_by,expires_at,created_at,updated_at)
                    VALUES (%s,%s,%s,%s,'VERIFIED',%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        driver_id, doc_type, f"{doc_type}-{i+1:06d}", f"s3://demo-driver-docs/{driver_id}/{doc_type.lower()}.jpg",
                        created_at, created_at + timedelta(hours=6), admin_id, (created_at + timedelta(days=365*3)).date(),
                        created_at, created_at + timedelta(hours=6),
                    ),
                )

            make, model, vtype, capacity = random.choice(makes)
            if vtype == "BIKE" and random.random() < 0.4:
                make, model, vtype, capacity = random.choice([("Yamaha","NMAX","BIKE",1),("Honda","Vario","BIKE",1),("Honda","Beat","BIKE",1)])
            vehicle_id = await fetch_scalar(
                conn,
                """
                INSERT INTO vehicle
                (license_plate,vehicle_make,vehicle_model,vehicle_year,vehicle_capacity,vehicle_color,vehicle_type,vehicle_status,verified_at,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s,%s)
                RETURNING vehicle_id
                """,
                (plate(), make, model, random.randint(2015, 2025), capacity, random.choice(["Black","White","Silver","Red","Blue"]), vtype, created_at + timedelta(days=1), created_at, created_at),
            )
            await conn.execute(
                """
                INSERT INTO driver_vehicle_assignment(driver_id,vehicle_id,assigned_from,assigned_to,is_active,created_at,updated_at)
                VALUES (%s,%s,%s,NULL,true,%s,%s)
                """,
                (driver_id, vehicle_id, created_at + timedelta(days=1), created_at + timedelta(days=1), created_at + timedelta(days=1)),
            )

        payment_type_ids = {}
        cur = await conn.execute("SELECT payment_method_type_id, method_code FROM payment_method_type")
        for row in await cur.fetchall():
            payment_type_ids[row[1]] = row[0]

        cur = await conn.execute("SELECT user_id FROM user_account WHERE username LIKE 'rider_%'")
        seeded_riders = [r[0] for r in await cur.fetchall()]
        for user_id in seeded_riders:
            n_methods = random.choice([1, 1, 2])
            methods = random.sample(list(payment_type_ids.items()), n_methods)
            for method_code, method_id in methods:
                await conn.execute(
                    """
                    INSERT INTO user_payment_method
                    (user_id,payment_method_type_id,provider_name,provider_customer_id,provider_payment_token,masked_account,expiry_month,expiry_year,is_default,payment_method_status,created_at,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s)
                    """,
                    (
                        user_id, method_id, "demo_gateway" if method_code != "CASH" else "cash",
                        f"cust_{user_id}", f"tok_{uuid.uuid4().hex}", "****" + str(random.randint(1000, 9999)),
                        random.randint(1, 12), random.randint(2027, 2035), True,
                        base_time, base_time,
                    ),
                )

        for i in range(SEED_PROMOTIONS):
            valid_from = clock.now() - timedelta(days=10)
            valid_to = clock.now() + timedelta(days=60)
            discount_type = random.choice(["PERCENT", "FIXED"])
            pct = money(random.choice([5, 10, 15, 20])) if discount_type == "PERCENT" else None
            fixed = money(random.choice([5000, 10000, 15000])) if discount_type == "FIXED" else None
            await conn.execute(
                """
                INSERT INTO promotion
                (promo_code,promo_description,discount_type,discount_pct,discount_amount,max_discount_amount,min_fare_amount,usage_limit_total,usage_limit_per_user,valid_from,valid_to,promotion_status,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s)
                ON CONFLICT (promo_code) DO NOTHING
                """,
                (
                    f"RIDE{i+1:02d}", f"Demo promo {i+1}", discount_type, pct, fixed,
                    money(random.choice([10000, 15000, 20000])), money(random.choice([20000, 30000, 40000])),
                    random.randint(200, 1000), random.randint(1, 3), valid_from, valid_to, base_time, base_time,
                ),
            )

        await conn.commit()
        logger.info("Seed completed")


async def load_pools(pool: AsyncConnectionPool):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT user_id FROM user_account WHERE username LIKE 'rider_%' AND account_status='ACTIVE'")
        riders = [r[0] for r in await cur.fetchall()]

        cur = await conn.execute(
            """
            SELECT dp.driver_id, dp.user_id, dva.vehicle_id, v.vehicle_type
            FROM driver_profile dp
            JOIN driver_vehicle_assignment dva ON dp.driver_id = dva.driver_id AND dva.is_active = true
            JOIN vehicle v ON dva.vehicle_id = v.vehicle_id
            WHERE dp.driver_status IN ('AVAILABLE','OFFLINE') AND dp.verification_status='VERIFIED'
            """
        )
        drivers = [DriverAssignment(*r) for r in await cur.fetchall()]

        cur = await conn.execute("SELECT promotion_id, discount_type, COALESCE(discount_pct,0), COALESCE(discount_amount,0), COALESCE(max_discount_amount,0), min_fare_amount FROM promotion WHERE promotion_status='ACTIVE'")
        promotions = await cur.fetchall()

        return riders, drivers, promotions


async def insert_status(conn, ride_id, old, new, changed_by, changed_at, reason_code=None, reason_note=None):
    await conn.execute(
        """
        INSERT INTO ride_status_history
        (ride_id,old_status,new_status,changed_by_user_id,reason_code,reason_note,changed_at,created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (ride_id, old, new, changed_by, reason_code, reason_note, changed_at, changed_at),
    )


def fare_breakdown(distance_km, duration_min, service_type, discount=Decimal("0.00")):
    if service_type == "BIKE":
        base, per_km, per_min = 7000, 2400, 250
    elif service_type == "XL":
        base, per_km, per_min = 15000, 5200, 600
    else:
        base, per_km, per_min = 10000, 3800, 450

    surge_multiplier = Decimal(str(random.choice([1.0, 1.0, 1.0, 1.1, 1.2, 1.35]))).quantize(Decimal("0.01"))
    base_fare = money(base)
    distance_fare = money(float(distance_km) * per_km)
    time_fare = money(float(duration_min) * per_min)
    before_surge = base_fare + distance_fare + time_fare
    surge_amount = money(float(before_surge) * (float(surge_multiplier) - 1))
    tax_amount = money(float(before_surge + surge_amount - discount) * 0.11)
    platform_fee = money(float(before_surge + surge_amount) * 0.18)
    total_fare = money(max(0, float(before_surge + surge_amount + tax_amount - discount)))
    driver_earning = money(max(0, float(total_fare - platform_fee)))
    return {
        "base_fare": base_fare, "distance_fare": distance_fare, "time_fare": time_fare,
        "surge_multiplier": surge_multiplier, "surge_amount": surge_amount,
        "discount_amount": discount, "tax_amount": tax_amount, "platform_fee": platform_fee,
        "driver_earning": driver_earning, "total_fare": total_fare,
    }


async def generate_one_ride(pool: AsyncConnectionPool, clock: SimClock, ride_no: int):
    riders, drivers, promotions = await load_pools(pool)
    if not riders or not drivers:
        logger.warning("Missing seed pools. riders=%s drivers=%s", len(riders), len(drivers))
        return

    rider_id = random.choice(riders)
    driver = random.choice(drivers)
    service_type = driver.vehicle_type
    requested_at = clock.now()

    distance_km = money(random.uniform(1.5, 38.0))
    duration_min = money(float(distance_km) * random.uniform(2.0, 4.5) + random.uniform(4.0, 12.0))
    pickup = rand_point()
    dropoff = rand_point()

    # Lifecycle durations in simulated minutes. These values create realistic DB timestamps.
    accept_delay = random.uniform(0.5, 6.5)
    arrive_delay = random.uniform(2.0, 18.0)
    pickup_wait = random.uniform(0.5, 7.0)
    ride_duration = max(5.0, float(duration_min) * random.uniform(0.75, 1.35))
    payment_delay = random.uniform(0.2, 4.0)
    review_delay = random.uniform(1.0, 180.0)

    outcome = random.choices(
        population=["COMPLETED", "RIDER_CANCEL_BEFORE_ACCEPT", "RIDER_CANCEL_AFTER_ACCEPT", "DRIVER_CANCEL", "PAYMENT_FAILED"],
        weights=[82, 4, 5, 4, 5],
        k=1,
    )[0]

    async with pool.connection() as conn:
        async with conn.transaction():
            ride_id = await fetch_scalar(
                conn,
                """
                INSERT INTO ride
                (rider_id,driver_id,vehicle_id,ride_status,service_type,city_code,requested_at,estimated_distance_km,estimated_duration_min,created_at,updated_at)
                VALUES (%s,NULL,NULL,'REQUESTED',%s,'JKT',%s,%s,%s,%s,%s)
                RETURNING ride_id
                """,
                (rider_id, service_type, requested_at, distance_km, duration_min, requested_at, requested_at),
            )
            await insert_status(conn, ride_id, None, "REQUESTED", rider_id, requested_at)
            await conn.execute(
                """
                INSERT INTO ride_location(ride_id,location_type,latitude,longitude,address_text,place_id,captured_at,created_at)
                VALUES
                (%s,'PICKUP_REQUESTED',%s,%s,%s,%s,%s,%s),
                (%s,'DROPOFF_REQUESTED',%s,%s,%s,%s,%s,%s)
                """,
                (
                    ride_id, pickup[0], pickup[1], fake.street_address(), f"place_{uuid.uuid4().hex[:12]}", requested_at, requested_at,
                    ride_id, dropoff[0], dropoff[1], fake.street_address(), f"place_{uuid.uuid4().hex[:12]}", requested_at, requested_at,
                ),
            )

    logger.info("Ride %s REQUESTED at %s", ride_id, requested_at.isoformat())
    await clock.sleep_sim_minutes(accept_delay)

    if outcome == "RIDER_CANCEL_BEFORE_ACCEPT":
        cancelled_at = requested_at + timedelta(minutes=random.uniform(1.0, 5.0))
        async with pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE ride
                    SET ride_status='CANCELLED', cancelled_at=%s, cancelled_by_user_id=%s,
                        cancel_reason_code='RIDER_CHANGED_MIND', cancel_reason_note='Rider cancelled before driver accepted',
                        updated_at=%s
                    WHERE ride_id=%s
                    """,
                    (cancelled_at, rider_id, cancelled_at, ride_id),
                )
                await insert_status(conn, ride_id, "REQUESTED", "CANCELLED", rider_id, cancelled_at, "RIDER_CHANGED_MIND")
        logger.info("Ride %s CANCELLED before accept at %s", ride_id, cancelled_at.isoformat())
        return

    accepted_at = requested_at + timedelta(minutes=accept_delay)
    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE ride
                SET ride_status='ACCEPTED', driver_id=%s, vehicle_id=%s, accepted_at=%s, updated_at=%s
                WHERE ride_id=%s
                """,
                (driver.driver_id, driver.vehicle_id, accepted_at, accepted_at, ride_id),
            )
            await conn.execute("UPDATE driver_profile SET driver_status='ON_RIDE', updated_at=%s WHERE driver_id=%s", (accepted_at, driver.driver_id))
            await insert_status(conn, ride_id, "REQUESTED", "ACCEPTED", driver.user_id, accepted_at)
    logger.info("Ride %s ACCEPTED at %s", ride_id, accepted_at.isoformat())

    if outcome in ("RIDER_CANCEL_AFTER_ACCEPT", "DRIVER_CANCEL"):
        cancel_wait = random.uniform(1.0, 8.0)
        await clock.sleep_sim_minutes(cancel_wait)
        cancelled_at = accepted_at + timedelta(minutes=cancel_wait)
        cancelled_by = rider_id if outcome == "RIDER_CANCEL_AFTER_ACCEPT" else driver.user_id
        reason = "RIDER_NO_LONGER_NEEDED" if outcome == "RIDER_CANCEL_AFTER_ACCEPT" else "DRIVER_VEHICLE_ISSUE"
        async with pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE ride
                    SET ride_status='CANCELLED', cancelled_at=%s, cancelled_by_user_id=%s,
                        cancel_reason_code=%s, cancel_reason_note=%s, updated_at=%s
                    WHERE ride_id=%s
                    """,
                    (cancelled_at, cancelled_by, reason, outcome.replace("_", " "), cancelled_at, ride_id),
                )
                await conn.execute("UPDATE driver_profile SET driver_status='AVAILABLE', updated_at=%s WHERE driver_id=%s", (cancelled_at, driver.driver_id))
                await insert_status(conn, ride_id, "ACCEPTED", "CANCELLED", cancelled_by, cancelled_at, reason)
        logger.info("Ride %s CANCELLED after accept at %s", ride_id, cancelled_at.isoformat())
        return

    await clock.sleep_sim_minutes(arrive_delay)
    arrived_at = accepted_at + timedelta(minutes=arrive_delay)
    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE ride SET ride_status='ARRIVED', arrived_at=%s, updated_at=%s WHERE ride_id=%s",
                (arrived_at, arrived_at, ride_id),
            )
            await conn.execute(
                """
                INSERT INTO ride_location(ride_id,location_type,latitude,longitude,address_text,place_id,captured_at,created_at)
                VALUES (%s,'PICKUP_ACTUAL',%s,%s,%s,%s,%s,%s)
                """,
                (ride_id, pickup[0], pickup[1], fake.street_address(), f"place_{uuid.uuid4().hex[:12]}", arrived_at, arrived_at),
            )
            await insert_status(conn, ride_id, "ACCEPTED", "ARRIVED", driver.user_id, arrived_at)

    await clock.sleep_sim_minutes(pickup_wait)
    started_at = arrived_at + timedelta(minutes=pickup_wait)
    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE ride SET ride_status='IN_PROGRESS', started_at=%s, updated_at=%s WHERE ride_id=%s",
                (started_at, started_at, ride_id),
            )
            await insert_status(conn, ride_id, "ARRIVED", "IN_PROGRESS", driver.user_id, started_at)

    # Insert tracking points while the ride is in progress. DB timestamps are spread over the actual simulated trip.
    points = min(MAX_TRACKING_POINTS_PER_RIDE, max(2, int(ride_duration // TRACKING_INTERVAL_SIM_MINUTES)))
    for idx in range(points):
        ratio = (idx + 1) / points
        recorded_at = started_at + timedelta(minutes=ride_duration * ratio)
        lat = interpolate(pickup[0], dropoff[0], ratio) + round(random.uniform(-0.0015, 0.0015), 6)
        lon = interpolate(pickup[1], dropoff[1], ratio) + round(random.uniform(-0.0015, 0.0015), 6)
        async with pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO ride_tracking_point
                    (ride_id,driver_id,latitude,longitude,speed_kmh,heading_degree,accuracy_meter,recorded_at,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        ride_id, driver.driver_id, round(lat, 6), round(lon, 6),
                        money(random.uniform(8, 55)), money(random.uniform(0, 359)), money(random.uniform(3, 18)),
                        recorded_at, recorded_at,
                    ),
                )
        await clock.sleep_sim_minutes(TRACKING_INTERVAL_SIM_MINUTES)

    completed_at = started_at + timedelta(minutes=ride_duration)
    fare = fare_breakdown(distance_km, duration_min, service_type)
    promo_row = random.choice(promotions) if promotions and random.random() < 0.25 else None
    discount = Decimal("0.00")
    if promo_row:
        promo_id, discount_type, pct, fixed, max_discount, min_fare = promo_row
        preliminary = fare["total_fare"]
        if preliminary >= min_fare:
            if discount_type == "PERCENT":
                discount = min(money(float(preliminary) * float(pct) / 100.0), max_discount)
            else:
                discount = min(fixed, max_discount)
            fare = fare_breakdown(distance_km, duration_min, service_type, discount)

    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE ride
                SET ride_status=%s, completed_at=%s, updated_at=%s
                WHERE ride_id=%s
                """,
                ("PAYMENT_FAILED" if outcome == "PAYMENT_FAILED" else "COMPLETED", completed_at, completed_at, ride_id),
            )
            await conn.execute(
                """
                INSERT INTO ride_location(ride_id,location_type,latitude,longitude,address_text,place_id,captured_at,created_at)
                VALUES (%s,'DROPOFF_ACTUAL',%s,%s,%s,%s,%s,%s)
                """,
                (ride_id, dropoff[0], dropoff[1], fake.street_address(), f"place_{uuid.uuid4().hex[:12]}", completed_at, completed_at),
            )
            await insert_status(conn, ride_id, "IN_PROGRESS", "COMPLETED" if outcome != "PAYMENT_FAILED" else "PAYMENT_FAILED", driver.user_id, completed_at)

            fare_id = await fetch_scalar(
                conn,
                """
                INSERT INTO ride_fare
                (ride_id,fare_type,fare_version,currency_code,distance_km,duration_min,base_fare,distance_fare,time_fare,
                 surge_multiplier,surge_amount,discount_amount,tax_amount,platform_fee,driver_earning,total_fare,fare_rule_code,calculated_at,created_at)
                VALUES (%s,'FINAL',1,'IDR',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING fare_id
                """,
                (
                    ride_id, distance_km, duration_min, fare["base_fare"], fare["distance_fare"], fare["time_fare"],
                    fare["surge_multiplier"], fare["surge_amount"], fare["discount_amount"], fare["tax_amount"],
                    fare["platform_fee"], fare["driver_earning"], fare["total_fare"], f"JKT_{service_type}_V1", completed_at, completed_at,
                ),
            )
            components = [
                ("BASE", "Base fare", fare["base_fare"]),
                ("DISTANCE", "Distance fare", fare["distance_fare"]),
                ("TIME", "Time fare", fare["time_fare"]),
                ("SURGE", "Surge amount", fare["surge_amount"]),
                ("DISCOUNT", "Discount", -fare["discount_amount"]),
                ("TAX", "Tax", fare["tax_amount"]),
                ("PLATFORM_FEE", "Platform fee", fare["platform_fee"]),
            ]
            for code, name, amount in components:
                await conn.execute(
                    """
                    INSERT INTO ride_fare_component(fare_id,component_code,component_name,component_amount,description,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    (fare_id, code, name, amount, f"{name} component", completed_at),
                )
            if promo_row and discount > 0:
                await conn.execute(
                    """
                    INSERT INTO promo_usage(promotion_id,ride_id,rider_id,discount_amount_applied,used_at,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (ride_id) DO NOTHING
                    """,
                    (promo_row[0], ride_id, rider_id, discount, completed_at, completed_at),
                )

            await conn.execute("UPDATE driver_profile SET driver_status='AVAILABLE', updated_at=%s WHERE driver_id=%s", (completed_at, driver.driver_id))

    await clock.sleep_sim_minutes(payment_delay)
    payment_at = completed_at + timedelta(minutes=payment_delay)

    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT user_payment_method_id
            FROM user_payment_method
            WHERE user_id=%s AND payment_method_status='ACTIVE'
            ORDER BY is_default DESC, user_payment_method_id
            LIMIT 1
            """,
            (rider_id,),
        )
        row = await cur.fetchone()
        user_payment_method_id = row[0] if row else None

        async with conn.transaction():
            status = "FAILED" if outcome == "PAYMENT_FAILED" else random.choices(["PAID", "FAILED"], [96, 4])[0]
            failure_code = None if status == "PAID" else random.choice(["INSUFFICIENT_BALANCE", "GATEWAY_TIMEOUT", "CARD_DECLINED"])
            provider_tx = f"pay_{uuid.uuid4().hex}" if status == "PAID" else None
            await conn.execute(
                """
                INSERT INTO payment_transaction
                (ride_id,user_payment_method_id,provider_name,provider_transaction_id,idempotency_key,amount,method_fee,currency_code,
                 payment_status,failure_code,failure_message,authorized_at,captured_at,paid_at,created_at,updated_at)
                VALUES (%s,%s,'demo_gateway',%s,%s,%s,%s,'IDR',%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    ride_id, user_payment_method_id, provider_tx, f"ride-{ride_id}-{uuid.uuid4().hex}",
                    fare["total_fare"], money(float(fare["total_fare"]) * 0.01),
                    status, failure_code, f"Demo {failure_code}" if failure_code else None,
                    payment_at if status == "PAID" else None, payment_at if status == "PAID" else None, payment_at if status == "PAID" else None,
                    payment_at, payment_at,
                ),
            )

    if outcome != "PAYMENT_FAILED" and random.random() < 0.65:
        await clock.sleep_sim_minutes(min(review_delay, 10))  # don't hold local demo tasks too long
        review_at = completed_at + timedelta(minutes=review_delay)
        async with pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO review
                    (ride_id,reviewer_id,reviewee_id,review_type,rating_score,comments,created_at,updated_at)
                    VALUES (%s,%s,%s,'RIDER_TO_DRIVER',%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        ride_id, rider_id, driver.user_id,
                        random.choices([5, 4, 3, 2, 1], [72, 20, 6, 1, 1])[0],
                        random.choice(["Good driver", "Clean vehicle", "Fast trip", "Smooth ride", None]),
                        review_at, review_at,
                    ),
                )

    logger.info("Ride %s finished outcome=%s requested=%s completed=%s", ride_id, outcome, requested_at.isoformat(), completed_at.isoformat())


async def ride_spawner(pool: AsyncConnectionPool, clock: SimClock):
    active = set()
    ride_no = 0
    spawn_interval_sim_min = 1.0 / max(RIDES_PER_MINUTE, 0.1)

    while True:
        active = {t for t in active if not t.done()}
        if len(active) < MAX_CONCURRENT_RIDES:
            ride_no += 1
            task = asyncio.create_task(generate_one_ride(pool, clock, ride_no))
            active.add(task)
            task.add_done_callback(lambda t: logger.exception("Ride task failed", exc_info=t.exception()) if t.exception() else None)
        await clock.sleep_sim_minutes(spawn_interval_sim_min)


async def main():
    clock = SimClock(SIM_START_DAYS_AGO, SIM_SECONDS_PER_MINUTE)
    async with AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=8, open=False) as pool:
        await pool.open()
        await seed_reference_data(pool, clock)
        logger.info("Generator started. Sim timestamp starts around %s", clock.now().isoformat())
        await ride_spawner(pool, clock)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Generator stopped")
