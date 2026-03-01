# Customer Data Pipeline

A data pipeline with 3 Docker services:

| Service | Description | Port |
|---|---|---|
| `mock-server` | Flask REST API serving mock customer data | 5000 |
| `pipeline-service` | FastAPI ingestion pipeline (dlt + SQLAlchemy) | 8000 |
| `postgres` | PostgreSQL database | 5432 |

**Flow:** Flask (JSON) → FastAPI (dlt ingest) → PostgreSQL → API Response

---

## Prerequisites

- Docker Desktop (running)
- `docker-compose --version` ≥ 2.x

---

## Quick Start

```bash
# Clone and enter the project
cd project-root

# Build and start all services
docker-compose up -d --build

# Check all services are healthy
docker-compose ps
```

---

## Testing All Endpoints

### Flask Mock Server (port 5000)

```bash
# Health check
curl http://localhost:5000/api/health

# Paginated customer list
curl "http://localhost:5000/api/customers?page=1&limit=5"

# Single customer
curl http://localhost:5000/api/customers/CUST-001
```

### FastAPI Pipeline Service (port 8000)

```bash
# Health check
curl http://localhost:8000/api/health

# Ingest data from Flask into PostgreSQL
curl -X POST http://localhost:8000/api/ingest

# Paginated results from database
curl "http://localhost:8000/api/customers?page=1&limit=5"

# Single customer from database
curl http://localhost:8000/api/customers/CUST-001
```

---

## Project Structure

```
project-root/
├── docker-compose.yml
├── README.md
├── mock-server/
│   ├── app.py
│   ├── data/customers.json
│   ├── Dockerfile
│   └── requirements.txt
└── pipeline-service/
    ├── main.py
    ├── database.py
    ├── models/
    │   ├── __init__.py
    │   └── customer.py
    ├── services/
    │   ├── __init__.py
    │   └── ingestion.py
    ├── Dockerfile
    └── requirements.txt
```

---

## API Reference

### Flask Mock Server

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/customers?page=1&limit=10` | Paginated customer list |
| GET | `/api/customers/{id}` | Single customer (404 if not found) |

### FastAPI Pipeline Service

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| POST | `/api/ingest` | Fetch all data from Flask and upsert to PostgreSQL |
| GET | `/api/customers?page=1&limit=10` | Paginated customers from DB |
| GET | `/api/customers/{id}` | Single customer from DB (404 if not found) |

---

## Database Schema

Table: `customers`

| Column | Type | Constraints |
|--------|------|-------------|
| customer_id | VARCHAR(50) | PRIMARY KEY |
| first_name | VARCHAR(100) | NOT NULL |
| last_name | VARCHAR(100) | NOT NULL |
| email | VARCHAR(255) | NOT NULL |
| phone | VARCHAR(20) | |
| address | TEXT | |
| date_of_birth | DATE | |
| account_balance | DECIMAL(15,2) | |
| created_at | TIMESTAMP | |

---

## Stopping Services

```bash
docker-compose down          # stop and remove containers
docker-compose down -v       # also remove the postgres volume
```
