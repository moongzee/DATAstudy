from kafka import KafkaProducer
import json
from data import get_registered_user
import time


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=['54.180.103.204:9092'], value_serializer=json_serializer)

if __name__ == "__main__":
    while 1 == 1:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send("test3", registered_user)
        time.sleep(4)