import apache_beam as beam
from datetime import datetime, timezone

# Rule-based fraud signals (extend with ML model later)
HIGH_RISK_COUNTRIES = {"NG", "RU", "CN"}
HIGH_AMOUNT_THRESHOLD = 5000.0
SUSPICIOUS_MERCHANTS = {"atm", "online_gaming"}

class DetectFraud(beam.DoFn):
    def process(self, record):
        signals = []
        risk_score = 0

        if record["amount"] > HIGH_AMOUNT_THRESHOLD:
            signals.append("HIGH_AMOUNT")
            risk_score += 40

        if record["country_code"] in HIGH_RISK_COUNTRIES:
            signals.append("HIGH_RISK_COUNTRY")
            risk_score += 30

        if record["merchant_category"] in SUSPICIOUS_MERCHANTS and record["is_online"]:
            signals.append("SUSPICIOUS_ONLINE_MERCHANT")
            risk_score += 20

        record["fraud_signals"] = signals
        record["risk_score"] = risk_score
        record["is_fraud_alert"] = risk_score >= 60
        record["processed_at"] = datetime.now(timezone.utc).isoformat()

        if record["is_fraud_alert"]:
            yield beam.pvalue.TaggedOutput("fraud", record)
        else:
            yield beam.pvalue.TaggedOutput("clean", record)
