import os
import sys
import time

from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv("DATABASE_URL")
MAX_ATTEMPTS = int(os.getenv("DB_INIT_MAX_ATTEMPTS", "30"))
RETRY_DELAY_SECONDS = float(os.getenv("DB_INIT_RETRY_DELAY", "5"))


def get_engine():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set.")
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def wait_for_database(engine) -> None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"Database connection established on attempt {attempt}.")
            return
        except Exception as exc:
            print(f"Database not ready yet (attempt {attempt}/{MAX_ATTEMPTS}): {exc}")
            if attempt == MAX_ATTEMPTS:
                raise
            time.sleep(RETRY_DELAY_SECONDS)


def validate_required_tables(engine) -> None:
    required_tables = ("transactions", "products")
    with engine.connect() as conn:
        for table_name in required_tables:
            conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1"))
    print("Analytics service database dependencies are available.")


if __name__ == "__main__":
    try:
        engine = get_engine()
        wait_for_database(engine)
        validate_required_tables(engine)
    except Exception as exc:
        print(f"Analytics service DB initialization failed: {exc}")
        sys.exit(1)
