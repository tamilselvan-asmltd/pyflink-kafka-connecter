import json
import time
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

message_template = {
    "name": "Flink doc",
    "size": 22,
    "unit": "bytes",
    "encoded": True,
    "content": "This is a test content"
}

if __name__ == "__main__":
    while True:
        producer.send('pwdtopic', message_template)
        print(f"Sent: {message_template}")
        time.sleep(3)
