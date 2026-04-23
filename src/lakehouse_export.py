import os
import duckdb
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from logger_config import setup_logger

load_dotenv()
logger = setup_logger()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT"))
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB   = os.getenv("POSTGRES_DB")

DUCKDB_PATH   = os.getenv("DUCKDB_PATH")


def get_postgres_connection():
    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASS,
        dbname=POSTGRES_DB,
    )
    logger.info("Postgres connection established")
    return pg_conn


def extract_gold(pg_conn) -> pd.DataFrame:
    query = """
        SELECT
            window_start,
            window_end,
            product_id,
            trade_count,
            vwap,
            low_price,
            high_price,
            total_volume,
            buy_volume,
            sell_volume
        FROM gold.trades_1min
        ORDER BY window_start DESC
    """
    trades_df = pd.read_sql(query, pg_conn)
    logger.info(f"Extracted {len(trades_df)} rows from Gold layer")
    return trades_df


def load_to_duckdb(trades_df: pd.DataFrame):
    db_con = duckdb.connect(DUCKDB_PATH)

    db_con.execute("""
        CREATE TABLE IF NOT EXISTS trades_1min (
            window_start    TIMESTAMPTZ,
            window_end      TIMESTAMPTZ,
            product_id      VARCHAR,
            trade_count     BIGINT,
            vwap            DOUBLE,
            low_price       DOUBLE,
            high_price      DOUBLE,
            total_volume    DOUBLE,
            buy_volume      DOUBLE,
            sell_volume     DOUBLE
        )
    """)

    db_con.execute("DELETE FROM trades_1min")
    db_con.execute("""INSERT INTO trades_1min
                    SELECT window_start,
                            window_end,
                            product_id,
                            trade_count,
                            vwap,
                            low_price,
                            high_price,
                            total_volume,
                            buy_volume,
                            sell_volume
                    FROM trades_df""")

    row_count = db_con.execute("SELECT COUNT(*) FROM trades_1min").fetchone()[0]
    logger.info(f"Loaded {row_count} rows into DuckDB")
    db_con.close()


def run():
    logger.info("Starting lakehouse export...")
    pg_conn = get_postgres_connection()

    try:
        trades_df = extract_gold(pg_conn)
        load_to_duckdb(trades_df)
        logger.success("Lakehouse export complete")
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise
    finally:
        pg_conn.close()
        logger.info("Postgres connection closed")


if __name__ == "__main__":
    run()