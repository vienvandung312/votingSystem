import random
from datetime import datetime
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.serializing_producer import SerializingProducer
from confluent_kafka.error import KafkaError, KafkaException
import json
from main import delivered_report, insert_to_db
from model import Candidate, Vote
from db import Database


CONF = {
    "bootstrap.servers": "localhost:9094"
}

producer = SerializingProducer(CONF)

consumer = DeserializingConsumer(CONF | {
    "group.id": "voting-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})
consumer.subscribe(['voter_topic'])

db = Database("postgresql://postgres:postgres@localhost/voting")

if __name__ == "__main__":
    with db.get_session().begin() as s:
        candidates = [candidate.to_dict() for candidate in s.query(Candidate).all()]

    if len(candidates) < 2:
        raise Exception("Not enough candidates")

    try:
        while True:
            msg = consumer.poll(0)
            if not msg:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode("utf-8"))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    "voting_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "vote": 1
                }
                try:
                    print("User {} is voting for {}".format(vote['voter_name'], vote['candidate_name']))
                    # TODO: Implement Atomicity for this operation
                    insert_to_db(db, Vote(
                                        voter_id=vote['voter_id'], 
                                        candidate_id=vote['candidate_id'], 
                                        voting_time=vote['voting_time'], 
                                        vote=vote['vote']
                                    )
                                )

                    producer.produce(
                        topic="votes_topic",
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivered_report
                    )
                    producer.poll(0)
                except KafkaException as e:
                    raise e
    except KafkaException as e:
        raise e