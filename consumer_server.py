from kafka import KafkaConsumer
import json
import time


def run_consumer():    
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=1000)
    
    consumer.subscribe(["police.service.calls"])
    for message in consumer:
        print(message.value)


if __name__ == "__main__":
    run_consumer()