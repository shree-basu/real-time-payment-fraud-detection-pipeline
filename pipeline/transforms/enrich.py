"""Pure and Beam enrichment transforms."""

from datetime import datetime

import apache_beam as beam

HIGH_RISK_COUNTRIES = frozenset({"CN", "NG", "RU"})


def enrich_transaction(record: dict) -> dict:
    enriched = dict(record)
    timestamp = datetime.fromisoformat(record["event_timestamp"].replace("Z", "+00:00"))
    enriched.update(
        {
            "event_date": timestamp.date().isoformat(),
            "hour_of_day": timestamp.hour,
            "is_high_risk_country": record["country_code"] in HIGH_RISK_COUNTRIES,
            "has_ip_address": bool(record.get("ip_address")),
        }
    )
    return enriched


class EnrichTransaction(beam.DoFn):
    def process(self, element: dict):
        yield enrich_transaction(element)
