from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures._base import TimeoutError

PRODUCERS_QUANTITY = 3
RUNNING_TIMER_IN_SECONDS = 20
ITERATION_INTERVAL = 2

running = True

def runProducer(producer, id):
    while running:
        data = {"value": int(datetime.utcnow().timestamp()), "producer": id+1}
        producer.send('topic_test', value=data)
        print(f"|{data}| sent")
        sleep(ITERATION_INTERVAL)
        pass

if __name__ == "__main__":
    producers = [KafkaProducer(bootstrap_servers='localhost:9092',
                               value_serializer=lambda x: dumps(x).encode('utf-8'))
                 for i in range(PRODUCERS_QUANTITY)]

    with ThreadPoolExecutor(max_workers=PRODUCERS_QUANTITY) as executor:
        try:
            list(
                executor.map(runProducer, producers, range(len(producers)),
                             timeout=RUNNING_TIMER_IN_SECONDS)
            )
        except TimeoutError:
            print("Done executing")
            running = False
            pass
    pass
