"""Deterministic transaction producer; stdout is the safe default."""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

SCENARIOS = ("normal", "duplicate", "invalid", "late", "fraud_burst")
DEFAULT_START = "2026-01-01T12:00:00Z"


def _rfc3339(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _transaction(seed: int, index: int, scenario: str, start: datetime, rng: random.Random) -> dict:
    event_time = start + timedelta(seconds=index * 15)
    account_id = f"acct-{rng.randint(1, 8):03d}"
    amount = Decimal(rng.randint(500, 450000)) / Decimal("100")
    country = rng.choice(["GB", "IN", "US", "US", "US"])
    category = rng.choice(["grocery", "retail", "travel", "utilities"])
    is_online = rng.choice([True, False])

    if scenario == "late":
        event_time -= timedelta(minutes=30)
    elif scenario == "fraud_burst":
        account_id = "acct-burst-001"
        amount = Decimal("7500.00") + index
        country = "NG"
        category = "electronics"
        is_online = True

    transaction_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"fraud-demo:{seed}:{scenario}:{index}"))
    return {
        "transaction_id": transaction_id,
        "account_id": account_id,
        "amount": format(amount, ".2f"),
        "currency": "USD",
        "merchant_category": category,
        "event_timestamp": _rfc3339(event_time),
        "country_code": country,
        "is_online": is_online,
        "ip_address": f"198.51.100.{(index % 200) + 1}" if is_online else None,
    }


def generate_events(
    *,
    seed: int,
    count: int,
    scenario: str,
    start_time: str = DEFAULT_START,
) -> list[str]:
    if count < 0:
        raise ValueError("count must be non-negative")
    if scenario not in SCENARIOS:
        raise ValueError(f"unsupported scenario: {scenario}")
    parsed_start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    if parsed_start.tzinfo is None:
        raise ValueError("start_time must include a timezone")
    rng = random.Random(seed)
    events: list[str] = []
    for index in range(count):
        if scenario == "duplicate" and index % 2 == 1:
            events.append(events[-1])
            continue
        if scenario == "invalid" and index % 2 == 0:
            events.append('{"transaction_id":"invalid-json"')
            continue
        event = _transaction(seed, index, scenario, parsed_start, rng)
        if scenario == "invalid":
            event["amount"] = "-1.00"
        events.append(json.dumps(event, sort_keys=True, separators=(",", ":")))
    return events


def _paced(events: Iterable[str], rate: float):
    if rate < 0:
        raise ValueError("rate must be non-negative")
    delay = 1 / rate if rate else 0
    for event in events:
        yield event
        if delay:
            time.sleep(delay)


def pubsub_attributes(event: str) -> dict[str, str]:
    """Return stable Pub/Sub attributes, including for malformed test events."""
    try:
        payload = json.loads(event)
        return {
            "transaction_id": payload["transaction_id"],
            "event_timestamp": payload["event_timestamp"],
        }
    except (json.JSONDecodeError, KeyError, TypeError):
        return {
            "transaction_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"invalid:{event}")),
            "event_timestamp": _rfc3339(
                datetime.fromisoformat(DEFAULT_START.replace("Z", "+00:00"))
            ),
        }


def publish_to_pubsub(
    events: Iterable[str], *, project: str, topic: str, confirmation: str
) -> None:
    if confirmation != "PUBSUB":
        raise ValueError("cloud publication blocked: pass --confirm-publish=PUBSUB deliberately")
    if not project or not topic:
        raise ValueError("--project and --topic are required in pubsub mode")

    # Deliberately imported only after the cloud-mode safety checks pass.
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = topic if topic.startswith("projects/") else publisher.topic_path(project, topic)
    futures = []
    for event in events:
        attributes = pubsub_attributes(event)
        futures.append(
            publisher.publish(
                topic_path,
                event.encode("utf-8"),
                **attributes,
            )
        )
    for future in futures:
        future.result()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--rate", type=float, default=0)
    parser.add_argument("--scenario", choices=SCENARIOS, default="normal")
    parser.add_argument("--start-time", default=DEFAULT_START)
    parser.add_argument("--mode", choices=("stdout", "pubsub"), default="stdout")
    parser.add_argument("--project")
    parser.add_argument("--topic")
    parser.add_argument("--confirm-publish", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    events = generate_events(
        seed=args.seed,
        count=args.count,
        scenario=args.scenario,
        start_time=args.start_time,
    )
    paced_events = _paced(events, args.rate)
    if args.mode == "stdout":
        for event in paced_events:
            print(event)
        return
    publish_to_pubsub(
        paced_events,
        project=args.project,
        topic=args.topic,
        confirmation=args.confirm_publish,
    )


if __name__ == "__main__":
    main()
