import os
import requests
import dlt
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from models.customer import Customer

MOCK_SERVER_URL = os.getenv("MOCK_SERVER_URL", "http://localhost:5000")


@dlt.resource(name="customers", primary_key="customer_id", write_disposition="merge")
def customers_resource():
    """
    dlt resource that fetches all customers from the Flask mock server
    with automatic pagination.
    """
    page = 1
    limit = 100

    while True:
        response = requests.get(
            f"{MOCK_SERVER_URL}/api/customers",
            params={"page": page, "limit": limit},
            timeout=10,
        )
        response.raise_for_status()
        result = response.json()

        records = result.get("data", [])
        if not records:
            break

        yield from records

        total = result.get("total", 0)
        if page * limit >= total:
            break

        page += 1


def ingest_customers(db: Session) -> int:
    """
    Uses the dlt resource to extract all customer records, then upserts
    them into PostgreSQL via SQLAlchemy.
    """
    all_records = list(customers_resource())

    if not all_records:
        return 0

    for record in all_records:
        stmt = (
            insert(Customer)
            .values(
                customer_id=record["customer_id"],
                first_name=record["first_name"],
                last_name=record["last_name"],
                email=record["email"],
                phone=record.get("phone"),
                address=record.get("address"),
                date_of_birth=record.get("date_of_birth"),
                account_balance=record.get("account_balance"),
                created_at=record.get("created_at"),
            )
            .on_conflict_do_update(
                index_elements=["customer_id"],
                set_={
                    "first_name": record["first_name"],
                    "last_name": record["last_name"],
                    "email": record["email"],
                    "phone": record.get("phone"),
                    "address": record.get("address"),
                    "date_of_birth": record.get("date_of_birth"),
                    "account_balance": record.get("account_balance"),
                    "created_at": record.get("created_at"),
                },
            )
        )
        db.execute(stmt)

    db.commit()
    return len(all_records)
