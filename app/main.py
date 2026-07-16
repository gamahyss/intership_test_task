import asyncio
import hashlib
import json
import os
import re
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field


CREATED = "CREATED"
PROCESSING = "PROCESSING"
COMPLETED = "COMPLETED"
REJECTED = "REJECTED"
FINAL_STATUSES = {COMPLETED, REJECTED}

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/candidate.sqlite3")
PROVIDER_URL = os.getenv("PROVIDER_URL", "http://localhost:8081").rstrip("/")
MAX_PROVIDER_ATTEMPTS = int(os.getenv("MAX_PROVIDER_ATTEMPTS", "8"))
DISPATCH_INTERVAL_SECONDS = float(os.getenv("DISPATCH_INTERVAL_SECONDS", "1"))
DISPATCH_STALE_SECONDS = int(os.getenv("DISPATCH_STALE_SECONDS", "30"))


class OperationCreate(BaseModel):
    operationId: str = Field(min_length=1, max_length=128)
    amount: str
    currency: str
    description: str | None = None


class Receipt(BaseModel):
    providerPaymentId: str = Field(min_length=1, max_length=128)
    operationId: str = Field(min_length=1, max_length=128)
    result: str
    message: str | None = None
    occurredAt: str | None = None


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def dt_to_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def validate_amount(value: str) -> None:
    if not re.fullmatch(r"[0-9]+(\.[0-9]{1,2})?", value):
        raise HTTPException(status_code=422, detail="amount must be a positive decimal string with up to two decimals")
    try:
        amount = Decimal(value)
    except InvalidOperation as exc:
        raise HTTPException(status_code=422, detail="amount is invalid") from exc
    if amount <= 0:
        raise HTTPException(status_code=422, detail="amount must be positive")


def validate_currency(value: str) -> None:
    if value != "RUB":
        raise HTTPException(status_code=422, detail="only RUB is supported")


def row_to_operation(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "operationId": row["operation_id"],
        "amount": row["amount"],
        "currency": row["currency"],
        "description": row["description"],
        "status": row["status"],
        "providerPaymentId": row["provider_payment_id"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
    }


def row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    payload = {
        "eventId": row["event_id"],
        "type": row["type"],
        "fromStatus": row["from_status"],
        "toStatus": row["to_status"],
        "message": row["message"],
        "occurredAt": row["occurred_at"],
    }
    if row["provider_payment_id"] is not None:
        payload["providerPaymentId"] = row["provider_payment_id"]
    if row["ignored"]:
        payload["ignored"] = True
    return payload


def connect() -> sqlite3.Connection:
    Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextmanager
def transaction(immediate: bool = True):
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS operations (
                operation_id TEXT PRIMARY KEY,
                amount TEXT NOT NULL,
                currency TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL CHECK (status IN ('CREATED', 'PROCESSING', 'COMPLETED', 'REJECTED')),
                provider_payment_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                submit_requested_at TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT,
                dispatching INTEGER NOT NULL DEFAULT 0,
                dispatch_claimed_at TEXT,
                provider_body TEXT
            );

            CREATE TABLE IF NOT EXISTS operation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id TEXT NOT NULL REFERENCES operations(operation_id) ON DELETE CASCADE,
                event_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                from_status TEXT,
                to_status TEXT,
                message TEXT NOT NULL,
                provider_payment_id TEXT,
                ignored INTEGER NOT NULL DEFAULT 0,
                occurred_at TEXT NOT NULL,
                UNIQUE(operation_id, event_id)
            );

            CREATE TABLE IF NOT EXISTS receipt_digests (
                digest TEXT PRIMARY KEY,
                operation_id TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_operations_dispatch
                ON operations(status, dispatching, next_attempt_at);
            """
        )
        stale_before = dt_to_iso(utc_now() - timedelta(seconds=DISPATCH_STALE_SECONDS))
        conn.execute(
            """
            UPDATE operations
               SET dispatching = 0, dispatch_claimed_at = NULL
             WHERE status = ? AND (dispatching = 1 OR dispatch_claimed_at < ?)
            """,
            (PROCESSING, stale_before),
        )


def add_event(
    conn: sqlite3.Connection,
    operation_id: str,
    event_type: str,
    from_status: str | None,
    to_status: str | None,
    message: str,
    *,
    provider_payment_id: str | None = None,
    ignored: bool = False,
    occurred_at: str | None = None,
) -> None:
    next_event_id = conn.execute(
        "SELECT COALESCE(MAX(event_id), 0) + 1 FROM operation_events WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO operation_events (
            operation_id, event_id, type, from_status, to_status, message,
            provider_payment_id, ignored, occurred_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            operation_id,
            next_event_id,
            event_type,
            from_status,
            to_status,
            message,
            provider_payment_id,
            1 if ignored else 0,
            occurred_at or iso_now(),
        ),
    )


def get_operation_or_404(conn: sqlite3.Connection, operation_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM operations WHERE operation_id = ?", (operation_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="operation not found")
    return row


def receipt_digest(receipt: Receipt) -> str:
    normalized = {
        "providerPaymentId": receipt.providerPaymentId,
        "operationId": receipt.operationId,
        "result": receipt.result,
        "message": receipt.message,
        "occurredAt": receipt.occurredAt,
    }
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def dispatch_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            claimed = claim_operation_for_dispatch()
            if claimed is None:
                await asyncio.wait_for(stop_event.wait(), timeout=DISPATCH_INTERVAL_SECONDS)
                continue
            await send_to_provider(claimed)
        except asyncio.TimeoutError:
            pass
        except Exception as exc:
            print(f"dispatch loop error: {exc}", flush=True)
            await asyncio.sleep(DISPATCH_INTERVAL_SECONDS)


def claim_operation_for_dispatch() -> dict[str, Any] | None:
    now = iso_now()
    stale_before = dt_to_iso(utc_now() - timedelta(seconds=DISPATCH_STALE_SECONDS))
    with transaction() as conn:
        conn.execute(
            """
            UPDATE operations
               SET dispatching = 0, dispatch_claimed_at = NULL
             WHERE status = ?
               AND dispatching = 1
               AND dispatch_claimed_at < ?
            """,
            (PROCESSING, stale_before),
        )
        row = conn.execute(
            """
            SELECT * FROM operations
             WHERE status = ?
               AND dispatching = 0
               AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
               AND attempt_count < ?
             ORDER BY submit_requested_at, operation_id
             LIMIT 1
            """,
            (PROCESSING, now, MAX_PROVIDER_ATTEMPTS),
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            """
            UPDATE operations
               SET dispatching = 1, dispatch_claimed_at = ?, attempt_count = attempt_count + 1
             WHERE operation_id = ?
            """,
            (now, row["operation_id"]),
        )
        body = json.loads(row["provider_body"])
        return {
            "operationId": row["operation_id"],
            "body": body,
            "attemptCount": row["attempt_count"] + 1,
        }


async def send_to_provider(claimed: dict[str, Any]) -> None:
    operation_id = claimed["operationId"]
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                f"{PROVIDER_URL}/payments",
                json=claimed["body"],
                headers={
                    "Idempotency-Key": operation_id,
                    "X-Correlation-ID": operation_id,
                },
            )
    except httpx.HTTPError as exc:
        release_dispatch(operation_id, retry=True, message=str(exc))
        return

    if response.status_code == 202:
        provider_payment_id = response.json().get("providerPaymentId")
        save_provider_response(operation_id, provider_payment_id)
        return

    if response.status_code == 503 or response.status_code >= 500:
        release_dispatch(operation_id, retry=True, message=f"provider returned {response.status_code}")
        return

    release_dispatch(operation_id, retry=True, message=f"unexpected provider status {response.status_code}")


def retry_delay(attempt_count: int) -> int:
    return min(60, 2 ** min(attempt_count, 5))


def release_dispatch(operation_id: str, *, retry: bool, message: str) -> None:
    with transaction() as conn:
        row = conn.execute("SELECT * FROM operations WHERE operation_id = ?", (operation_id,)).fetchone()
        if row is None or row["status"] in FINAL_STATUSES:
            return
        next_attempt = dt_to_iso(utc_now() + timedelta(seconds=retry_delay(row["attempt_count"]))) if retry else None
        conn.execute(
            """
            UPDATE operations
               SET dispatching = 0, dispatch_claimed_at = NULL, next_attempt_at = ?, updated_at = ?
             WHERE operation_id = ?
            """,
            (next_attempt, iso_now(), operation_id),
        )
        add_event(conn, operation_id, "PROVIDER_RETRY_SCHEDULED", PROCESSING, PROCESSING, message, ignored=True)


def save_provider_response(operation_id: str, provider_payment_id: str | None) -> None:
    with transaction() as conn:
        row = conn.execute("SELECT * FROM operations WHERE operation_id = ?", (operation_id,)).fetchone()
        if row is None:
            return
        if provider_payment_id and row["provider_payment_id"] not in (None, provider_payment_id):
            conn.execute(
                """
                UPDATE operations
                   SET dispatching = 0, dispatch_claimed_at = NULL, updated_at = ?
                 WHERE operation_id = ?
                """,
                (iso_now(), operation_id),
            )
            add_event(
                conn,
                operation_id,
                "PROVIDER_RESPONSE_CONFLICT",
                row["status"],
                row["status"],
                "Provider response had different providerPaymentId",
                provider_payment_id=provider_payment_id,
                ignored=True,
            )
            return

        conn.execute(
            """
            UPDATE operations
               SET provider_payment_id = COALESCE(provider_payment_id, ?),
                   dispatching = 0,
                   dispatch_claimed_at = NULL,
                   next_attempt_at = NULL,
                   updated_at = ?
             WHERE operation_id = ?
            """,
            (provider_payment_id, iso_now(), operation_id),
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    stop_event = asyncio.Event()
    task = asyncio.create_task(dispatch_loop(stop_event))
    try:
        yield
    finally:
        stop_event.set()
        await task


app = FastAPI(title="Payment proof service", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/operations", status_code=201)
def create_operation(payload: OperationCreate) -> dict[str, Any]:
    validate_amount(payload.amount)
    validate_currency(payload.currency)
    now = iso_now()
    provider_body = json.dumps(
        {
            "operationId": payload.operationId,
            "amount": payload.amount,
            "currency": payload.currency,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    with transaction() as conn:
        existing = conn.execute(
            "SELECT 1 FROM operations WHERE operation_id = ?",
            (payload.operationId,),
        ).fetchone()
        if existing is not None:
            raise HTTPException(status_code=409, detail="operation already exists")
        conn.execute(
            """
            INSERT INTO operations (
                operation_id, amount, currency, description, status, provider_payment_id,
                created_at, updated_at, provider_body
            )
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                payload.operationId,
                payload.amount,
                payload.currency,
                payload.description,
                CREATED,
                now,
                now,
                provider_body,
            ),
        )
        add_event(conn, payload.operationId, CREATED, None, CREATED, "Operation created", occurred_at=now)
        row = get_operation_or_404(conn, payload.operationId)
        return row_to_operation(row)


@app.post("/operations/{operation_id}/submit")
def submit_operation(operation_id: str, response: Response) -> dict[str, Any]:
    with transaction() as conn:
        row = get_operation_or_404(conn, operation_id)
        if row["status"] != CREATED:
            response.status_code = 200
            return row_to_operation(row)

        now = iso_now()
        conn.execute(
            """
            UPDATE operations
               SET status = ?, submit_requested_at = ?, next_attempt_at = ?,
                   dispatching = 0, dispatch_claimed_at = NULL, updated_at = ?
             WHERE operation_id = ? AND status = ?
            """,
            (PROCESSING, now, now, now, operation_id, CREATED),
        )
        add_event(conn, operation_id, PROCESSING, CREATED, PROCESSING, "Payment submission requested", occurred_at=now)
        response.status_code = 202
        row = get_operation_or_404(conn, operation_id)
        return row_to_operation(row)


@app.post("/receipts", status_code=204)
def receive_receipt(payload: Receipt) -> Response:
    if payload.result not in FINAL_STATUSES:
        raise HTTPException(status_code=422, detail="result must be COMPLETED or REJECTED")

    digest = receipt_digest(payload)
    with transaction() as conn:
        row = get_operation_or_404(conn, payload.operationId)

        if row["provider_payment_id"] is not None and row["provider_payment_id"] != payload.providerPaymentId:
            raise HTTPException(status_code=409, detail="providerPaymentId does not match operation")

        inserted = conn.execute(
            "INSERT OR IGNORE INTO receipt_digests (digest, operation_id, created_at) VALUES (?, ?, ?)",
            (digest, payload.operationId, iso_now()),
        ).rowcount

        if inserted == 0:
            return Response(status_code=204)

        if row["status"] in FINAL_STATUSES:
            if row["status"] != payload.result:
                add_event(
                    conn,
                    payload.operationId,
                    "RECEIPT_IGNORED",
                    row["status"],
                    row["status"],
                    payload.message or f"Late conflicting receipt ignored: {payload.result}",
                    provider_payment_id=payload.providerPaymentId,
                    ignored=True,
                    occurred_at=payload.occurredAt,
                )
            return Response(status_code=204)

        now = iso_now()
        conn.execute(
            """
            UPDATE operations
               SET status = ?,
                   provider_payment_id = COALESCE(provider_payment_id, ?),
                   dispatching = 0,
                   dispatch_claimed_at = NULL,
                   next_attempt_at = NULL,
                   updated_at = ?
             WHERE operation_id = ?
            """,
            (payload.result, payload.providerPaymentId, now, payload.operationId),
        )
        add_event(
            conn,
            payload.operationId,
            payload.result,
            row["status"],
            payload.result,
            payload.message or f"Payment {payload.result.lower()}",
            provider_payment_id=payload.providerPaymentId,
            occurred_at=payload.occurredAt,
        )
    return Response(status_code=204)


@app.get("/operations/{operation_id}")
def get_operation(operation_id: str) -> dict[str, Any]:
    with transaction(immediate=False) as conn:
        row = get_operation_or_404(conn, operation_id)
        return row_to_operation(row)


@app.get("/operations/{operation_id}/events")
def get_events(operation_id: str) -> list[dict[str, Any]]:
    with transaction(immediate=False) as conn:
        get_operation_or_404(conn, operation_id)
        rows = conn.execute(
            """
            SELECT * FROM operation_events
             WHERE operation_id = ?
             ORDER BY event_id
            """,
            (operation_id,),
        ).fetchall()
        return [row_to_event(row) for row in rows]
