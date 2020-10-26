from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

PRODUCERS_QUANTITY = 3
RUN_FOR_SECONDS = 30

producers = [ KafkaProducer(bootstrap_servers='localhost:9092',
                           value_serializer=lambda x: dumps(x).encode('utf-8')) 
                           for i in range(PRODUCERS_QUANTITY) ]


def runProducer(producer, id):
    print(f"here id {id}")
    while True:
        data = {"value": datetime.now(), "producer": id}
        producer.send('topic_test', value=data)
        print(f"|{data}| sent")
        sleep(2)


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=PRODUCERS_QUANTITY) as executor:
        # , timeout = RUN_FOR_SECONDS)
        executor.map(runProducer, producers, range(1, len(producers)+1))
    pass
