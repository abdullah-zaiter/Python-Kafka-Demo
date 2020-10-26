from kafka import KafkaConsumer
from json import loads
from concurrent.futures import ThreadPoolExecutor, as_completed, thread
from concurrent.futures._base import TimeoutError

CONSUMERS_QUANTITY = 3
TOPIC_NAME = "topic_test3"
running = True

def consumerVisualizer(consumer, id): 
    for message in consumer:
        print(f"|{message.value}| received on consumer {id+1}")

if __name__ == "__main__":
    consumers = [KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id=f"group",
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    ) for i in range(CONSUMERS_QUANTITY)]

    with ThreadPoolExecutor(max_workers=CONSUMERS_QUANTITY) as executor:
        list(executor.map(consumerVisualizer, consumers, range(len(consumers))))
    pass