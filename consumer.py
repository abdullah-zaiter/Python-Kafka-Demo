from kafka import KafkaConsumer
from json import loads
from concurrent.futures import ThreadPoolExecutor, as_completed, thread
from concurrent.futures._base import TimeoutError
import itertools as it

CONSUMERS_QUANTITY = 3

RUNNING_TIMER_IN_SECONDS = 15


running = True

def consumerVisualizer(consumer, id): 
    for message in consumer:
        print(f"|{message.value}| received on consumer {id+1}")

if __name__ == "__main__":
    consumers = [KafkaConsumer(
        'topic_test',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id=f"group-{i}",
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    ) for i in range(CONSUMERS_QUANTITY)]

    with ThreadPoolExecutor(max_workers=CONSUMERS_QUANTITY) as executor:
        try:
            list(executor.map(consumerVisualizer, consumers, range(len(consumers)), timeout= RUNNING_TIMER_IN_SECONDS))
        except TimeoutError:
            print("Done executing")
            running = False
            SystemExit(0)
            pass
    pass