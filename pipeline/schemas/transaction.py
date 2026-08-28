"""Strict transaction event contract and invalid-event envelope."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

REQUIRED_FIELDS = {
    "transaction_id",
    "account_id",
    "amount",
    "currency",
    "merchant_category",
    "event_timestamp",
    "country_code",
    "is_online",
}
SUPPORTED_CURRENCIES = frozenset({"USD", "EUR", "GBP", "INR"})
SUPPORTED_MERCHANT_CATEGORIES = frozenset(
    {"electronics", "grocery", "jewelry", "retail", "travel", "utilities"}
)
COUNTRY_CODE_PATTERN = re.compile(r"^[A-Z]{2}$")
MAX_NUMERIC_AMOUNT = Decimal("99999999999999999999999999999.999999999")


@dataclass(frozen=True)
class ContractViolation(ValueError):
    code: str
    reason: str

    def __str__(self) -> str:
        return f"{self.code}: {self.reason}"


def _require_nonempty_string(record: Mapping[str, Any], field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ContractViolation("INVALID_TYPE", f"{field} must be a non-empty string")
    return value.strip()


def normalize_event_timestamp(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractViolation("INVALID_TIMESTAMP", "event_timestamp must be a string")
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ContractViolation("INVALID_TIMESTAMP", "event_timestamp is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ContractViolation("INVALID_TIMESTAMP", "event_timestamp must include a timezone")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_amount(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ContractViolation("INVALID_AMOUNT", "amount must be numeric")
    try:
        amount = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ContractViolation("INVALID_AMOUNT", "amount must be a finite decimal") from exc
    if not amount.is_finite() or amount <= 0:
        raise ContractViolation("INVALID_AMOUNT", "amount must be finite and greater than zero")
    if abs(amount) > MAX_NUMERIC_AMOUNT:
        raise ContractViolation("INVALID_AMOUNT", "amount exceeds BigQuery NUMERIC range")
    return amount.quantize(Decimal("0.000000001"))


def parse_transaction(payload: bytes | str | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(payload, Mapping):
        record = dict(payload)
    else:
        try:
            record = json.loads(payload, parse_float=Decimal, parse_int=Decimal)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
            raise ContractViolation("MALFORMED_JSON", "payload is not valid JSON") from exc
    if not isinstance(record, dict):
        raise ContractViolation("INVALID_SHAPE", "payload must be a JSON object")
    missing = sorted(REQUIRED_FIELDS.difference(record))
    if missing:
        raise ContractViolation("MISSING_FIELD", f"missing required fields: {', '.join(missing)}")

    transaction_id = _require_nonempty_string(record, "transaction_id")
    account_id = _require_nonempty_string(record, "account_id")
    currency = _require_nonempty_string(record, "currency").upper()
    if currency not in SUPPORTED_CURRENCIES:
        raise ContractViolation("UNSUPPORTED_CURRENCY", f"unsupported currency: {currency}")
    category = _require_nonempty_string(record, "merchant_category").lower()
    if category not in SUPPORTED_MERCHANT_CATEGORIES:
        raise ContractViolation("UNSUPPORTED_CATEGORY", f"unsupported category: {category}")
    country_code = _require_nonempty_string(record, "country_code").upper()
    if not COUNTRY_CODE_PATTERN.fullmatch(country_code):
        raise ContractViolation("INVALID_COUNTRY_CODE", "country_code must be two letters")
    if not isinstance(record["is_online"], bool):
        raise ContractViolation("INVALID_BOOLEAN", "is_online must be a JSON boolean")
    ip_address = record.get("ip_address")
    if ip_address is not None and (not isinstance(ip_address, str) or not ip_address.strip()):
        raise ContractViolation("INVALID_TYPE", "ip_address must be null or a non-empty string")

    return {
        "transaction_id": transaction_id,
        "account_id": account_id,
        "amount": parse_amount(record["amount"]),
        "currency": currency,
        "merchant_category": category,
        "event_timestamp": normalize_event_timestamp(record["event_timestamp"]),
        "country_code": country_code,
        "is_online": record["is_online"],
        "ip_address": ip_address.strip() if isinstance(ip_address, str) else None,
    }


def raw_payload_text(payload: Any) -> str:
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, default=str, sort_keys=True)


def invalid_event_envelope(
    payload: Any,
    violation: ContractViolation,
    *,
    transaction_id: str | None = None,
    event_timestamp: str | None = None,
) -> dict[str, Any]:
    return {
        "raw_payload": raw_payload_text(payload),
        "transaction_id": transaction_id,
        "event_timestamp": event_timestamp,
        "error_code": violation.code,
        "error_reason": violation.reason,
        "processing_timestamp": datetime.now(UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
    }
