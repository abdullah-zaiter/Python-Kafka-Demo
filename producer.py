from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures._base import TimeoutError
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError

if __name__ == "__main__":
   
    pass
