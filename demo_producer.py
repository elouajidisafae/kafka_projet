"""
Producteur de démo — rythme réaliste.
"""
import time
import random
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "kafka:29092"
TOPICS = ["orders", "clicks", "logs", "events"]

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
print("Producteur démarré")

counter = 0
while True:
    for topic in TOPICS:
        for _ in range(5):      # 5 messages par topic (au lieu de 100)
            counter += 1
            msg = f"msg-{topic}-{counter}-{random.randint(1000,10_000 - 1)}"
            producer.produce(topic, value=msg.encode())
        producer.flush()
    print(f"[producer] {counter} messages envoyés")
    time.sleep(3)               # toutes les 3 secondes (au lieu de 2)