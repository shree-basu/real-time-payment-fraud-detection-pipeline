import apache_beam as beam
from datetime import datetime

class EnrichTransaction(beam.DoFn):
    def process(self, record):
        try:
            dt = datetime.strptime(record['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
            record['hour_of_day'] = dt.hour
        except Exception:
            record['hour_of_day'] = -1
        yield record 

    