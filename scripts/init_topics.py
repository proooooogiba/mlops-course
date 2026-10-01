from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from app.settings import BOOTSTRAP_SERVERS, TRANSACTIONS_TOPIC, SCORES_TOPIC, DLQ_TOPIC

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
try:
    for topic in (TRANSACTIONS_TOPIC, SCORES_TOPIC, DLQ_TOPIC):
        try:
            admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
            print(f"Created {topic}", flush=True)
        except TopicAlreadyExistsError:
            print(f"Already exists: {topic}", flush=True)
finally:
    admin.close()
