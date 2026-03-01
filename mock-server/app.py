from flask import Flask, jsonify, request, abort
import json
import os
import math

app = Flask(__name__)

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "customers.json")

def load_customers():
    with open(DATA_FILE, "r") as f:
        return json.load(f)

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "mock-server"}), 200

@app.route("/api/customers", methods=["GET"])
def get_customers():
    customers = load_customers()
    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 10))
    except ValueError:
        return jsonify({"error": "Invalid page or limit parameter"}), 400

    if page < 1 or limit < 1:
        return jsonify({"error": "page and limit must be positive integers"}), 400

    total = len(customers)
    start = (page - 1) * limit
    end = start + limit
    paginated = customers[start:end]

    return jsonify({
        "data": paginated,
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": math.ceil(total / limit)
    }), 200

@app.route("/api/customers/<customer_id>", methods=["GET"])
def get_customer(customer_id):
    customers = load_customers()
    customer = next((c for c in customers if c["customer_id"] == customer_id), None)
    if not customer:
        return jsonify({"error": f"Customer '{customer_id}' not found"}), 404
    return jsonify({"data": customer}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
