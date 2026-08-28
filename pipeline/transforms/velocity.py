"""Event-time sliding windows and accumulating late panes for velocity fraud."""

from datetime import UTC, datetime
from decimal import Decimal, localcontext

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import trigger, window
from apache_beam.utils.windowed_value import PaneInfoTiming


class VelocityCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return 0, Decimal("0"), Decimal("0")

    def add_input(self, accumulator, record):
        count, total, maximum = accumulator
        amount = record["amount"]
        with localcontext() as context:
            context.prec = 76
            updated_total = total + amount
        return count + 1, updated_total, max(maximum, amount)

    def merge_accumulators(self, accumulators):
        count = 0
        total = Decimal("0")
        maximum = Decimal("0")
        for item_count, item_total, item_maximum in accumulators:
            count += item_count
            with localcontext() as context:
                context.prec = 76
                total += item_total
            maximum = max(maximum, item_maximum)
        return count, total, maximum

    def extract_output(self, accumulator):
        count, total, maximum = accumulator
        return {
            "transaction_count": count,
            "total_amount": total,
            "maximum_amount": maximum,
        }


class FormatVelocityPane(beam.DoFn):
    def __init__(self, *, count_threshold: int, amount_threshold: Decimal) -> None:
        self.count_threshold = count_threshold
        self.amount_threshold = amount_threshold
        self.alerts = Metrics.counter("fraud_pipeline", "velocity_alerts")

    def process(
        self,
        element,
        beam_window=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        (account_id, currency), aggregate = element
        if (
            aggregate["transaction_count"] < self.count_threshold
            and aggregate["total_amount"] < self.amount_threshold
        ):
            return
        self.alerts.inc()
        start = beam_window.start.to_utc_datetime().replace(tzinfo=UTC)
        end = beam_window.end.to_utc_datetime().replace(tzinfo=UTC)
        timing = PaneInfoTiming.to_string(pane_info.timing)
        yield {
            "account_id": account_id,
            "currency": currency,
            "window_start": start.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "window_end": end.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "window_date": start.date().isoformat(),
            "pane_index": pane_info.index,
            "pane_timing": timing,
            "is_late": pane_info.timing == PaneInfoTiming.LATE,
            **aggregate,
            "processed_at": datetime.now(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
        }


class BuildVelocityAlerts(beam.PTransform):
    def __init__(
        self,
        *,
        window_duration_seconds: int,
        window_period_seconds: int,
        allowed_lateness_seconds: int,
        count_threshold: int,
        amount_threshold: Decimal,
    ) -> None:
        self.window_duration_seconds = window_duration_seconds
        self.window_period_seconds = window_period_seconds
        self.allowed_lateness_seconds = allowed_lateness_seconds
        self.count_threshold = count_threshold
        self.amount_threshold = amount_threshold

    def expand(self, scored):
        return (
            scored
            | "KeyVelocityByAccountAndCurrency"
            >> beam.Map(lambda record: ((record["account_id"], record["currency"]), record))
            | "SlidingEventTimeWindow"
            >> beam.WindowInto(
                window.SlidingWindows(
                    size=self.window_duration_seconds,
                    period=self.window_period_seconds,
                ),
                trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=self.allowed_lateness_seconds,
            )
            | "AggregateVelocity" >> beam.CombinePerKey(VelocityCombineFn())
            | "FormatVelocityAlerts"
            >> beam.ParDo(
                FormatVelocityPane(
                    count_threshold=self.count_threshold,
                    amount_threshold=self.amount_threshold,
                )
            )
        )
