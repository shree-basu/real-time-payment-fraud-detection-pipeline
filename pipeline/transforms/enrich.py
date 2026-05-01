import apache_beam as beam
from datetime import datetime
HIGH_RISK_COUNTRIES = {"NG","RU","CN"}

class EnrichTransaction(beam.DoFn):
    def process(self, record):
        try:
            dt = datetime.strptime(record['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
            record['hour_of_day'] = dt.hour
        except Exception:
            record['hour_of_day'] = -1
            
        record['is_high_risk_country'] = record['country_code'] in HIGH_RISK_COUNTRIES
        
        record['has_ip_address'] = record.get('ip_address') is not None
        yield record 

    