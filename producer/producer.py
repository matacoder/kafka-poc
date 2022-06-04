from time import sleep
from json import dumps
from kafka import KafkaProducer

print("Connecting to KAFKA")

producer = KafkaProducer(
    bootstrap_servers=["127.0.0.1:29092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)
print("Connected to KAFKA")

for e in range(1000):
    data = {"number": e}
    producer.send("numtest", value=data)
    print(f"sent {data}")
    sleep(5)
