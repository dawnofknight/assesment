"""
Integration tests for the Flask Mock Server (port 5000)
and the FastAPI Pipeline Service (port 8000).

Run:
    pip install pytest requests
    pytest tests/ -v
"""
import time
import math

import pytest
import requests

FLASK_BASE = "http://localhost:5001"
FASTAPI_BASE = "http://localhost:8001"

TOTAL_CUSTOMERS = 22          # number of records in customers.json
FIRST_CUSTOMER_ID = "CUST-001"
BAD_CUSTOMER_ID = "CUST-NOTEXIST"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wait_for_service(url: str, retries: int = 15, delay: float = 2.0) -> bool:
    """Poll a health endpoint until it returns 200 or retries are exhausted."""
    for _ in range(retries):
        try:
            r = requests.get(url, timeout=3)
            if r.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(delay)
    return False


# ---------------------------------------------------------------------------
# Session-scoped fixtures that confirm services are reachable
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def flask_ready():
    assert wait_for_service(f"{FLASK_BASE}/api/health"), (
        "Flask mock server did not become healthy in time. "
        "Make sure docker-compose is running."
    )


@pytest.fixture(scope="session", autouse=True)
def fastapi_ready():
    assert wait_for_service(f"{FASTAPI_BASE}/api/health"), (
        "FastAPI pipeline service did not become healthy in time. "
        "Make sure docker-compose is running."
    )


# ===========================================================================
# FLASK MOCK SERVER TESTS
# ===========================================================================

class TestFlaskHealth:
    def test_health_returns_200(self):
        r = requests.get(f"{FLASK_BASE}/api/health")
        assert r.status_code == 200

    def test_health_body(self):
        r = requests.get(f"{FLASK_BASE}/api/health")
        body = r.json()
        assert body["status"] == "ok"


class TestFlaskCustomersList:
    def test_default_pagination(self):
        r = requests.get(f"{FLASK_BASE}/api/customers")
        assert r.status_code == 200
        body = r.json()
        assert "data" in body
        assert "total" in body
        assert "page" in body
        assert "limit" in body

    def test_total_count(self):
        r = requests.get(f"{FLASK_BASE}/api/customers?limit=100")
        body = r.json()
        assert body["total"] == TOTAL_CUSTOMERS

    def test_page_and_limit(self):
        r = requests.get(f"{FLASK_BASE}/api/customers?page=1&limit=5")
        assert r.status_code == 200
        body = r.json()
        assert len(body["data"]) == 5
        assert body["page"] == 1
        assert body["limit"] == 5

    def test_second_page(self):
        r = requests.get(f"{FLASK_BASE}/api/customers?page=2&limit=5")
        assert r.status_code == 200
        body = r.json()
        assert len(body["data"]) == 5

    def test_last_page_partial(self):
        limit = 5
        total_pages = math.ceil(TOTAL_CUSTOMERS / limit)
        r = requests.get(f"{FLASK_BASE}/api/customers?page={total_pages}&limit={limit}")
        assert r.status_code == 200
        body = r.json()
        expected = TOTAL_CUSTOMERS - (total_pages - 1) * limit
        assert len(body["data"]) == expected

    def test_beyond_last_page_empty(self):
        r = requests.get(f"{FLASK_BASE}/api/customers?page=9999&limit=10")
        assert r.status_code == 200
        assert r.json()["data"] == []

    def test_customer_fields_present(self):
        r = requests.get(f"{FLASK_BASE}/api/customers?page=1&limit=1")
        customer = r.json()["data"][0]
        for field in ["customer_id", "first_name", "last_name", "email",
                      "phone", "address", "date_of_birth", "account_balance", "created_at"]:
            assert field in customer, f"Missing field: {field}"


class TestFlaskCustomerById:
    def test_existing_customer(self):
        r = requests.get(f"{FLASK_BASE}/api/customers/{FIRST_CUSTOMER_ID}")
        assert r.status_code == 200
        body = r.json()
        assert "data" in body
        assert body["data"]["customer_id"] == FIRST_CUSTOMER_ID

    def test_missing_customer_404(self):
        r = requests.get(f"{FLASK_BASE}/api/customers/{BAD_CUSTOMER_ID}")
        assert r.status_code == 404

    def test_missing_customer_error_body(self):
        r = requests.get(f"{FLASK_BASE}/api/customers/{BAD_CUSTOMER_ID}")
        body = r.json()
        assert "error" in body


# ===========================================================================
# FASTAPI PIPELINE SERVICE TESTS
# ===========================================================================

class TestFastAPIHealth:
    def test_health_returns_200(self):
        r = requests.get(f"{FASTAPI_BASE}/api/health")
        assert r.status_code == 200

    def test_health_body(self):
        body = requests.get(f"{FASTAPI_BASE}/api/health").json()
        assert body["status"] == "ok"


class TestFastAPIIngest:
    @pytest.fixture(scope="class", autouse=True)
    def do_ingest(self):
        """Run ingestion once before all tests in this class."""
        r = requests.post(f"{FASTAPI_BASE}/api/ingest", timeout=30)
        assert r.status_code == 200, f"Ingest failed: {r.text}"
        self.__class__._ingest_response = r.json()

    def test_ingest_status_success(self):
        assert self._ingest_response["status"] == "success"

    def test_ingest_records_count(self):
        assert self._ingest_response["records_processed"] == TOTAL_CUSTOMERS

    def test_ingest_idempotent(self):
        """Second ingest (upsert) should succeed without duplicates."""
        r = requests.post(f"{FASTAPI_BASE}/api/ingest", timeout=30)
        assert r.status_code == 200
        assert r.json()["status"] == "success"
        assert r.json()["records_processed"] == TOTAL_CUSTOMERS


class TestFastAPICustomersList:
    @pytest.fixture(scope="class", autouse=True)
    def ensure_ingested(self):
        requests.post(f"{FASTAPI_BASE}/api/ingest", timeout=30)

    def test_list_returns_200(self):
        r = requests.get(f"{FASTAPI_BASE}/api/customers")
        assert r.status_code == 200

    def test_response_shape(self):
        body = requests.get(f"{FASTAPI_BASE}/api/customers").json()
        for key in ["data", "total", "page", "limit", "total_pages"]:
            assert key in body, f"Missing key: {key}"

    def test_total_after_ingest(self):
        body = requests.get(f"{FASTAPI_BASE}/api/customers?limit=100").json()
        assert body["total"] == TOTAL_CUSTOMERS

    def test_pagination_limit(self):
        r = requests.get(f"{FASTAPI_BASE}/api/customers?page=1&limit=5")
        assert r.status_code == 200
        body = r.json()
        assert len(body["data"]) == 5
        assert body["page"] == 1

    def test_page_2(self):
        r = requests.get(f"{FASTAPI_BASE}/api/customers?page=2&limit=5")
        assert r.status_code == 200
        assert len(r.json()["data"]) == 5

    def test_customer_fields(self):
        body = requests.get(f"{FASTAPI_BASE}/api/customers?page=1&limit=1").json()
        customer = body["data"][0]
        for field in ["customer_id", "first_name", "last_name", "email"]:
            assert field in customer


class TestFastAPICustomerById:
    @pytest.fixture(scope="class", autouse=True)
    def ensure_ingested(self):
        requests.post(f"{FASTAPI_BASE}/api/ingest", timeout=30)

    def test_existing_customer(self):
        r = requests.get(f"{FASTAPI_BASE}/api/customers/{FIRST_CUSTOMER_ID}")
        assert r.status_code == 200
        body = r.json()
        assert body["data"]["customer_id"] == FIRST_CUSTOMER_ID

    def test_all_fields_in_response(self):
        body = requests.get(f"{FASTAPI_BASE}/api/customers/{FIRST_CUSTOMER_ID}").json()
        customer = body["data"]
        for field in ["customer_id", "first_name", "last_name", "email",
                      "phone", "address", "date_of_birth", "account_balance", "created_at"]:
            assert field in customer, f"Missing field: {field}"

    def test_missing_customer_404(self):
        r = requests.get(f"{FASTAPI_BASE}/api/customers/{BAD_CUSTOMER_ID}")
        assert r.status_code == 404

    def test_missing_customer_detail(self):
        body = requests.get(f"{FASTAPI_BASE}/api/customers/{BAD_CUSTOMER_ID}").json()
        assert "detail" in body
