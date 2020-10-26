from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures._base import TimeoutError
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError

RUNNING_TIMER_IN_SECONDS = 120
PRODUCERS_QUANTITY = 3
ITERATION_INTERVAL = 2
TOPIC_NAME = "topic_test3"
PARTITIONS_QUANTITY = 3

running = True

def runProducer(producer, id):
    while running:
        data = {"value": int(datetime.utcnow().timestamp()), "producer": id+1}
        producer.send(TOPIC_NAME, value=data)
        print(f"|{data}| sent")
        sleep(ITERATION_INTERVAL)
        pass

if __name__ == "__main__":
    producers = [KafkaProducer(bootstrap_servers='localhost:9092',
                               value_serializer=lambda x: dumps(x).encode('utf-8'))
                 for i in range(PRODUCERS_QUANTITY)]
    
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")                                                                                                                                                               
    topics = admin_client.list_topics()
    if TOPIC_NAME not in topics:
        admin_client.create_topics([NewTopic(name=TOPIC_NAME, num_partitions=PARTITIONS_QUANTITY, replication_factor=1)], validate_only=False)

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
