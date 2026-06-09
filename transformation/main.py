"""Transform bronze.events -> silver.payments -> gold.fact_revenue in DuckDB via PySpark."""

from pathlib import Path

import duckdb
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = str(ROOT / "data" / "analytics.duckdb")
SILVER_OUT = str(ROOT / "data" / "silver" / "payments")
GOLD_OUT = str(ROOT / "data" / "gold" / "fact_revenue")

# fixed rates to EUR
RATES = {"EUR": 1.0, "USD": 0.92, "GBP": 1.17}


def _spark() -> SparkSession:
    return SparkSession.builder.appName("payments-transform").getOrCreate()


def _read_bronze(spark: SparkSession):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    result = con.execute("SELECT * FROM bronze.events")
    cols = [c[0] for c in result.description]
    rows = [dict(zip(cols, r)) for r in result.fetchall()]
    con.close()
    return spark.createDataFrame(rows)


def _to_silver(df):
    rate_expr = (
        F.when(F.col("currency") == "USD", RATES["USD"])
        .when(F.col("currency") == "GBP", RATES["GBP"])
        .otherwise(RATES["EUR"])
    )
    return (
        df.withColumn("payment_date", F.to_date(F.col("payment_date")))
        .withColumn("price_eur", F.col("price") * rate_expr)
        .select(
            "transaction_id",
            "game",
            "payment_date",
            "status",
            "currency",
            "price",
            "price_eur",
        )
    )


def _to_gold(silver):
    return (
        silver.groupBy("payment_date", "game", "status")
        .agg(
            F.round(F.sum("price_eur"), 2).alias("revenue"),
            F.count("*").alias("count"),
        )
        .orderBy("payment_date", "game", "status")
    )


def _write_duckdb() -> tuple[int, int]:
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.payments AS
        SELECT * FROM read_parquet('{SILVER_OUT}/*.parquet')
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE gold.fact_revenue AS
        SELECT * FROM read_parquet('{GOLD_OUT}/*.parquet')
    """)
    silver_n = con.execute("SELECT count(*) FROM silver.payments").fetchone()[0]
    gold_n = con.execute("SELECT count(*) FROM gold.fact_revenue").fetchone()[0]
    con.close()
    return silver_n, gold_n


def main() -> None:
    spark = _spark()
    bronze = _read_bronze(spark)
    silver = _to_silver(bronze)
    gold = _to_gold(silver)

    Path(SILVER_OUT).mkdir(parents=True, exist_ok=True)
    Path(GOLD_OUT).mkdir(parents=True, exist_ok=True)
    silver.write.mode("overwrite").parquet(SILVER_OUT)
    gold.write.mode("overwrite").parquet(GOLD_OUT)
    spark.stop()

    silver_n, gold_n = _write_duckdb()
    print(f"silver.payments: {silver_n} rows")
    print(f"gold.fact_revenue: {gold_n} rows")


if __name__ == "__main__":
    main()
