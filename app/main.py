"""FastAPI app: generate fake payment events and write to date-partitioned S3."""

import json
import os
import random
from datetime import date, timedelta

import boto3
from fastapi import FastAPI
from pydantic import BaseModel

BUCKET = os.getenv("S3_BUCKET", "demo-payment-bucket-2026")
REGION = os.getenv("AWS_DEFAULT_REGION", "eu-central-1")
ROLE_ARN = os.getenv("AWS_ROLE_ARN", "arn:aws:iam::856611477482:role/payments-s3-role")
GAMES = ["funny-game", "arcade-game", "puzzle-game"]
CURRENCIES = ["EUR", "USD", "GBP"]
STATUSES = ["success", "failed", "pending"]

app = FastAPI()


class PaymentEvent(BaseModel):
    currency: str
    game: str
    payment_date: str
    price: float
    status: str
    transaction_id: str


def _s3():
    base = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=REGION,
    )
    creds = base.client("sts").assume_role(
        RoleArn=ROLE_ARN, RoleSessionName="payments-api"
    )["Credentials"]
    return boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )


def _fake_event(payment_date: date) -> PaymentEvent:
    return PaymentEvent(
        currency=random.choice(CURRENCIES),
        game=random.choice(GAMES),
        payment_date=payment_date.isoformat(),
        price=round(random.uniform(0.99, 99.99), 2),
        status=random.choice(STATUSES),
        transaction_id=str(random.randint(100000000000, 999999999999)),
    )


def _write_s3(events: list[PaymentEvent]) -> str:
    key = f"payments/date={events[0].payment_date}/{events[0].transaction_id}.json"
    print(events)
    _s3().put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps([event.model_dump() for event in events]),
        ContentType="application/json",
    )
    return key


@app.post("/payments", response_model=PaymentEvent)
def create_payment(payment_date: str | None = None):
    d = date.fromisoformat(payment_date) if payment_date else date.today()
    event = _fake_event(d)
    _write_s3(event)
    return event


@app.post("/payments/seed-month")
def seed_month(start_date: str = "2026-01-01"):
    start = date.fromisoformat(start_date)
    for i in range(30):
        keys = []
        d = start + timedelta(days=i)
        for _ in range(random.randint(5, 50)):
            keys.append(_fake_event(d))
        _write_s3(keys)
    return {"days": 30, "events": len(keys), "bucket": BUCKET}
