from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

print("Connecting to KAFKA")

consumer = KafkaConsumer(
    "numtest",
    bootstrap_servers=["127.0.0.1:29092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)

print("Connected to KAFKA")
print("Connecting to MONGO")

client = MongoClient("mongodb://root:example@localhost:27017")

print("Connected to MONGO")
collection = client.numtest.numtest


print("I am trying")
for message in consumer:
    print("Get first")
    message = message.value
    collection.insert_one(message)
    print("{} added to {}".format(message, collection))
