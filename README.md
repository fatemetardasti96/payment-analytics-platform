# Demo Payment Analytics Platform

For comprehensive design decisions and doc please refer to the [architecture docs](docs/adr/0001-payments-analytics-architecture.md)

### How to run the app to generate data

0. export environment variables 

1. start the server

`uv run fastapi dev app/main.py`

2. send a curl request to write the monthly data into S3

`curl -X POST http://127.0.0.1:8000/payments/seed-month`

or to generate data for a specific month

```
  curl -X POST \
  "http://localhost:8000/payments/seed-month?start_date=2026-03-01" \
  -H "Content-Type: application/json"
```

### Run ingestion script

`uv run python ingestion/main.py`

### Run transformation script

`uv run python transformation/main.py`