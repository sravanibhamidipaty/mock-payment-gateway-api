# 🚀 Mock Payment Gateway API

A high-performance, asynchronous, and containerized Payment Gateway API built with Python and FastAPI.

This project demonstrates production-grade backend architecture, including asynchronous database operations, idempotency handling for financial transactions, and automated testing.

---

## 🛠️ Tech Stack

- **Framework:** FastAPI (Python 3.12)
- **Database:** PostgreSQL (asyncpg)
- **ORM:** SQLAlchemy 2.0 (Async)
- **Testing:** Pytest (pytest-asyncio, httpx)
- **Deployment:** Docker & Docker Compose

---

## ✨ Core Features

### 1. Asynchronous Architecture
Utilizes non-blocking I/O for high-throughput request handling.

### 2. Idempotency Protection
Prevents duplicate network charges by verifying `Idempotency-Key` headers against the PostgreSQL database. Duplicate requests return a `409 Conflict` response.

### 3. Security
Enforces API Key authorization via FastAPI dependency injection.

### 4. Data Validation
Uses strict Pydantic schemas for all incoming payment payloads.

---

## 📦 Quickstart (One-Command Run)

You do **not** need Python or PostgreSQL installed on your machine to run this API. You only need Docker installed.

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd <repository-name>
```

### 2. Start the Application

```bash
docker compose up --build
```

The API will start and be available at:

```text
http://localhost:8000
```

The PostgreSQL database is exposed locally on:

```text
localhost:5433
```

---

## 📖 Interactive API Documentation

FastAPI automatically generates Swagger UI documentation.

Open:

```text
http://localhost:8000/docs
```

You can explore and test all available endpoints directly from the browser.

---

## 💻 Usage Example

Once the Docker containers are running, test the payment endpoint using:

```bash
curl -X POST http://127.0.0.1:8000/users/999/charges \
-H "x-api-key: sk_test_12345" \
-H "idempotency-key: test-key-123" \
-H "Content-Type: application/json" \
-d '{"amount":50,"currency":"USD","description":"Test Charge"}'
```

### Expected Behavior

**First Request**

```json
{
  "id": 1,
  "user_id": 999,
  "amount": 50,
  "currency": "USD",
  "description": "Test Charge",
  "status": "success"
}
```

**Second Request (Same Idempotency Key)**

```json
{
  "detail": "Duplicate request detected"
}
```

Response Status:

```http
409 Conflict
```

---

## 🧪 Running Automated Tests

To run the full suite of asynchronous integration tests:

### 1. Create and Activate a Virtual Environment

```bash
python -m venv .venv
```

#### macOS/Linux

```bash
source .venv/bin/activate
```

#### Windows

```bash
.venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run Tests

```bash
pytest
```

---

## 📁 Example Project Structure

```text
.
├── app/
│   ├── api/
│   ├── core/
│   ├── db/
│   ├── models/
│   ├── schemas/
│   └── main.py
├── tests/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## 🔐 Required Headers

| Header | Description |
|----------|-------------|
| `x-api-key` | API authentication key |
| `idempotency-key` | Prevents duplicate charges |
| `Content-Type` | Must be `application/json` |

---

## 📝 Sample Request Body

```json
{
  "amount": 50,
  "currency": "USD",
  "description": "Test Charge"
}
```

---

## 🎯 Project Goals

This project demonstrates:

- Async FastAPI application architecture
- Async PostgreSQL database access
- SQLAlchemy 2.0 async patterns
- API key authentication
- Idempotent payment processing
- Dockerized local development
- Automated integration testing
- Production-oriented backend design

---

## 📄 License

This project is provided for educational and portfolio purposes.