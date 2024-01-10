from kafka import KafkaProducer
import datetime
import pytz
import time
import random
import json

if __name__ == "__main__":
    TOPIC_NAME = "sample-logs"
    BROKERS = ["172.31.36.132:9092"]
    
    def get_seoul_datetime():
        utc_now = pytz.utc.localize(datetime.datetime.utcnow())
        kst_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))
        
        seoul_datetime = kst_now.strftime("%Y-%m-%d %H:%M:%S")
        
        return seoul_datetime

    def produce_log_from_file():
        
        producer = KafkaProducer(bootstrap_servers=BROKERS)
        
        with open("/home/ubuntu/working/spark-examples/spark-streaming/data/sample_log.json", 'r') as f:
            lines = f.readlines()
            
        for line in lines:
            log_data = json.loads(line)
            
            data = {
                "host": log_data['host'],
                "user-identifier": log_data['user-identifier'],
                "datetime": get_seoul_datetime(),
                'method': log_data['method'],
                'request': log_data['request'],
                'protocol': log_data['protocol'],
                'status': log_data['status'],
                'bytes': log_data['bytes'],
                'referer': log_data['referer']
            }
            
            log = json.dumps(data).encode('utf-8')
            producer.send(TOPIC_NAME, log)
            producer.flush()
            print(data)
            
            time.sleep(1)
    
    produce_log_from_file()