import json
import random
import requests
from requests.exceptions import ConnectionError
from typing import Any, Tuple
from confluent_kafka.serializing_producer import SerializingProducer
from model import Candidate, Voter, Vote
from db import Database
from sqlalchemy.ext.declarative import DeclarativeMeta

def create_tables(db: Database) -> None:
    db.create_table(Candidate)
    db.create_table(Voter)
    db.create_table(Vote)

def generate_candidates() -> Tuple:
    try:
        response = requests.get(BASE_URL)
    except ConnectionError:
        raise ConnectionError("Check Internet connection")
    
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return (
            Candidate(
                candidate_id=user_data['login']['uuid'],
                candidate_name=f"{user_data['name']['first']} {user_data['name']['last']}",
                party_affiliation=PARTIES[random.randint(0, len(PARTIES) - 1)],
                place_of_birth=user_data['location']['timezone']['description'],
                promises=user_data['login']['sha256'],
                photo_url=user_data['picture']['large']
            ),
            {
                'candidate_id': user_data['login']['uuid'],
                'candidate_name': f"{user_data['name']['first']} {user_data['name']['last']}",
                'party_affiliation': PARTIES[random.randint(0, len(PARTIES) - 1)],
                'place_of_birth': user_data['location']['timezone']['description'],
                'promises': user_data['login']['sha256'],
                'photo_url': user_data['picture']['large']
            }
        )
    else:
        # HACK: This is a bad practice, should be handled properly
        raise Exception ("Error fetching data")


def generate_voter_data() -> Tuple:
    try:
        response = requests.get(BASE_URL)
    except ConnectionError:
        raise ConnectionError("Check Internet connection")
    
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return (
            Voter(
                voter_id=user_data['login']['uuid'],
                voter_name=f"{user_data['name']['first']} {user_data['name']['last']}",
                date_of_birth=user_data['dob']['date'],
                gender=user_data['gender'],
                nationality=user_data['nat'],
                registration_number=user_data['login']['username'],
                address_street=f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                address_city=user_data['location']['city'],
                address_state=user_data['location']['state'],
                address_country=user_data['location']['country'],
                address_postcode=user_data['location']['postcode'],
                email=user_data['email'],
                phone_number=user_data['phone'],
                picture=user_data['picture']['large'],
                registered_age=user_data['registered']['age']
            ),
            {
                'voter_id': user_data['login']['uuid'],
                'voter_name': f"{user_data['name']['first']} {user_data['name']['last']}",
                'date_of_birth': user_data['dob']['date'],
                'gender': user_data['gender'],
                'nationality': user_data['nat'],
                'registration_number': user_data['login']['username'],
                'address_street': f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                'address_city': user_data['location']['city'],
                'address_state': user_data['location']['state'],
                'address_country': user_data['location']['country'],
                'address_postcode': user_data['location']['postcode'],
                'email': user_data['email'],
                'phone_number': user_data['phone'],
                'picture': user_data['picture']['large'],
                'registered_age': user_data['registered']['age']
            }
        )
    else:
        # HACK: This is a bad practice, should be handled properly
        raise Exception ("Error fetching data")


def insert_to_db(db: Database, data: DeclarativeMeta) -> None:
    with db.get_session().begin() as s:
        s.add(data)
        s.commit()

def delivered_report(err, msg):
    if err:
        print(f"Message delivery failed: {msg}")
    else:
        print(f"Message delivery successfully to topic {msg.topic()} at partition {msg.partition()}")


if __name__ == "__main__":
    BASE_URL = "https://randomuser.me/api/?nat=gb"
    PARTIES = ['Bear', 'Tiger', 'Eagle']

    db = Database("postgresql://postgres:postgres@localhost/voting")
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9094'})

    create_tables(db)

    with db.get_session().begin() as s:
        candidates_num = s.query(Candidate).distinct(Candidate.candidate_id).count()
    
    for _ in range(3 - candidates_num):
        candidate = generate_candidates()
        insert_to_db(db, candidate[0]) # HACK: Using tuple to store 2 data representation, this is a bad practice

    for i in range(1000):
        voter_data = generate_voter_data()
        insert_to_db(db,voter_data[0])

        producer.produce(
            topic="voter_topic",
            key=voter_data[1]['voter_id'],
            value=json.dumps(voter_data[1]),
            on_delivery=delivered_report,
        )
        print(f"Produced voter number {i}, data: {voter_data[1]}")
        producer.flush()
