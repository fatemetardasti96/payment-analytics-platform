"""ETL: read payment JSON from S3, write Parquet, load bronze.events in DuckDB."""

import os
from pathlib import Path

import boto3
import duckdb
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent
BUCKET = os.getenv("S3_BUCKET", "demo-payment-bucket-2026")
REGION = os.getenv("AWS_DEFAULT_REGION", "eu-central-1")
ROLE_ARN = os.getenv("AWS_ROLE_ARN", "arn:aws:iam::856611477482:role/payments-s3-role")
S3_INPUT = f"s3a://{BUCKET}/payments/"
PARQUET_OUT = str(ROOT / "data" / "bronze" / "events")
DUCKDB_PATH = str(ROOT / "data" / "analytics.duckdb")

SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.4.1,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.367"
)


def _assume_role_creds() -> tuple[str, str, str]:
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=REGION,
    )
    creds = session.client("sts").assume_role(
        RoleArn=ROLE_ARN, RoleSessionName="payments-etl"
    )["Credentials"]
    return creds["AccessKeyId"], creds["SecretAccessKey"], creds["SessionToken"]


def _spark() -> SparkSession:
    key, secret, token = _assume_role_creds()
    return (
        SparkSession.builder.appName("payments-etl")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", key)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.session.token", token)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        .getOrCreate()
    )


def _read_events(spark: SparkSession):
    return (
        spark.read.json(S3_INPUT)
    )


def _write_parquet(df) -> None:
    Path(PARQUET_OUT).mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").partitionBy("date").parquet(PARQUET_OUT)


def _create_bronze_table() -> int:
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze.events AS
        SELECT * FROM read_parquet(
            '{PARQUET_OUT}/**/*.parquet'
        )
    """)
    count = con.execute("SELECT count(*) FROM bronze.events").fetchone()[0]
    con.close()
    return count


def main() -> None:
    spark = _spark()
    df = _read_events(spark)
    df.show(5, truncate=False)
    _write_parquet(df)
    spark.stop()
    rows = _create_bronze_table()
    print(f"Parquet: {PARQUET_OUT}")
    print(f"DuckDB: {DUCKDB_PATH} -> bronze.events ({rows} rows)")


if __name__ == "__main__":
    main()
