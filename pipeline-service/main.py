import math
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from database import engine, get_db, Base, SessionLocal
from models.customer import Customer
from services.ingestion import ingest_customers

logger = logging.getLogger("pipeline")
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup
    Base.metadata.create_all(bind=engine)
    # Auto-seed: ingest from Flask if the table is empty
    db = SessionLocal()
    try:
        count = db.query(Customer).count()
        if count == 0:
            logger.info("No data found in DB — running initial seed from Flask...")
            n = ingest_customers(db)
            logger.info(f"Seeded {n} customer records into PostgreSQL.")
        else:
            logger.info(f"DB already has {count} records, skipping auto-seed.")
    except Exception as exc:
        logger.warning(f"Auto-seed failed (Flask may not be ready yet): {exc}")
    finally:
        db.close()
    yield


app = FastAPI(title="Customer Pipeline Service", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    return {"status": "ok", "service": "pipeline-service"}


# ---------------------------------------------------------------------------
# POST /api/ingest
# ---------------------------------------------------------------------------

@app.post("/api/ingest")
def ingest(db: Session = Depends(get_db)):
    """Fetch all customers from the Flask mock server and upsert into PostgreSQL."""
    try:
        records_processed = ingest_customers(db)
        return {"status": "success", "records_processed": records_processed}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# GET /api/customers
# ---------------------------------------------------------------------------

@app.get("/api/customers")
def list_customers(
    page: int = Query(default=1, ge=1, description="Page number"),
    limit: int = Query(default=10, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db),
):
    """Return paginated list of customers from PostgreSQL."""
    total = db.query(Customer).count()
    offset = (page - 1) * limit
    customers = db.query(Customer).offset(offset).limit(limit).all()

    return {
        "data": [_serialize(c) for c in customers],
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": math.ceil(total / limit) if total > 0 else 0,
    }


# ---------------------------------------------------------------------------
# GET /api/customers/{id}
# ---------------------------------------------------------------------------

@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)):
    """Return a single customer by ID."""
    customer = db.query(Customer).filter(Customer.customer_id == customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found")
    return {"data": _serialize(customer)}


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _serialize(customer: Customer) -> dict:
    return {
        "customer_id": customer.customer_id,
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "email": customer.email,
        "phone": customer.phone,
        "address": customer.address,
        "date_of_birth": str(customer.date_of_birth) if customer.date_of_birth else None,
        "account_balance": float(customer.account_balance) if customer.account_balance is not None else None,
        "created_at": customer.created_at.isoformat() if customer.created_at else None,
    }
