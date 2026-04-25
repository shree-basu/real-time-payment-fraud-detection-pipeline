from dataclasses import dataclass
from typing import Optional

@dataclass
class Transaction:
    transaction_id: str
    account_id: str
    amount: float
    currency: str
    merchant_category: str
    timestamp: str
    country_code: str
    is_online: bool
    ip_address: Optional[str] = None

REQUIRED_FIELDS = [
    "transaction_id", "account_id", "amount",
    "currency", "merchant_category", "timestamp",
    "country_code", "is_online"
]
