"""Configurable, rule-only transaction fraud scoring."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import apache_beam as beam
from apache_beam.metrics import Metrics


@dataclass(frozen=True)
class FraudRuleConfig:
    high_amount_threshold: Decimal = Decimal("5000")
    high_amount_points: int = 40
    high_risk_country_points: int = 30
    suspicious_online_points: int = 20
    alert_threshold: int = 60
    suspicious_categories: frozenset[str] = frozenset({"electronics", "jewelry", "travel"})


def score_transaction(
    record: dict,
    config: FraudRuleConfig,
    *,
    processed_at: str | None = None,
) -> dict:
    score = 0
    signals: list[str] = []
    if record["amount"] > config.high_amount_threshold:
        score += config.high_amount_points
        signals.append("HIGH_AMOUNT")
    if record["is_high_risk_country"]:
        score += config.high_risk_country_points
        signals.append("HIGH_RISK_COUNTRY")
    if record["is_online"] and record["merchant_category"] in config.suspicious_categories:
        score += config.suspicious_online_points
        signals.append("SUSPICIOUS_ONLINE_CATEGORY")

    scored = dict(record)
    scored.update(
        {
            "fraud_signals": signals,
            "risk_score": score,
            "is_fraud_alert": score >= config.alert_threshold,
            "processed_at": processed_at
            or datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        }
    )
    return scored


class ScoreTransaction(beam.DoFn):
    def __init__(self, config: FraudRuleConfig) -> None:
        self.config = config
        self.rule_alerts = Metrics.counter("fraud_pipeline", "rule_fraud_alerts")

    def process(self, element: dict):
        scored = score_transaction(element, self.config)
        if scored["is_fraud_alert"]:
            self.rule_alerts.inc()
        yield scored


# Compatibility alias for the original public module name.
DetectFraud = ScoreTransaction
