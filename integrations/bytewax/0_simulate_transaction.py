import json
import hopsworks

from confluent_kafka import Producer

from transactions import get_transactions
from hsfs_bytewax_util import get_kafka_config

# coonect to hopsworks
project = hopsworks.login()

# setup kafka producer
KAFKA_TOPIC_NAME = "credit_card_transactions"

kafka_api = project.get_kafka_api()
#kafka_config = kafka_api.get_default_config()
fs = project.get_feature_store()
kafka_config = get_kafka_config(fs.id)

print(kafka_config)
producer = Producer(kafka_config)

# simulate transactions
inp = get_transactions()

# send to source topic
for transaction in inp:
    producer.produce(KAFKA_TOPIC_NAME, json.dumps(transaction))
    producer.flush()
