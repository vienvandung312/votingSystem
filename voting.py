import random
from datetime import datetime
from db import SingletonDBConnection
from confluent_kafka import Consumer, SerializingProducer, KafkaError, KafkaException
import json
import time

from main import delivered_report

conf = {
    "bootstrap.servers": "localhost:9094"
}
consumer = Consumer(conf | {
    "group.id": "voting-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

producer = SerializingProducer(conf)

if __name__ == "__main__":

    pg_conn = SingletonDBConnection(host="localhost", dbname="voting", user="postgres", password="postgres").connect()
    cursor = pg_conn.cursor()
    candidate_query = cursor.execute(
        """
        SELECT row_to_json(t1)
        FROM
        (SELECT * FROM candidates) t1;
        """
    )
    candidates = [candidate[0] for candidate in cursor.fetchall()]

    if len(candidates) < 2:
        raise Exception("Not enough candidates")

    consumer.subscribe(['voter_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=0.1)
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
                    cursor.execute(
                        """
                        INSERT INTO votes(voter_id, candidate_id, voting_time)
                        VALUES(%s,%s,%s)
                        """,
                        (vote['voter_id'], vote['candidate_id'], vote['voting_time'])
                    )
                    pg_conn.commit()

                    producer.produce(
                        topic="votes_topic",
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivered_report
                    )
                    producer.poll(0)
                except Exception as e:
                    print(e)
            time.sleep(0.5)
    except Exception as e:
        raise e
