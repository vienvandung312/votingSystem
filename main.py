import json
from db import SingletonDBConnection
import random
import requests
from typing import *
from confluent_kafka import SerializingProducer

BASE_URL = "https://randomuser.me/api/?nat=gb"
PARTIES = ['Bear', 'Tiger', 'Eagle']
random.seed(42)


def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT         
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY, 
            voter_name VARCHAR (255), 
            date_of_birth DATE, 
            gender VARCHAR (255), 
            nationality VARCHAR(255), 
            registration_number VARCHAR(255), 
            address_street VARCHAR(255), 
            address_city VARCHAR(255), 
            address_state VARCHAR(255), 
            address_country VARCHAR(255), 
            address_postcode VARCHAR (255), 
            email VARCHAR (255), 
            phone_number VARCHAR (255), 
            picture TEXT,
            registered_age INTEGER
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote INT DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
        """
    )
    conn.commit()
    cursor.close()


def generate_candidates(candidate_number, total_parties):
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            'candidate_id': user_data['login']['uuid'],
            'candidate_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": "A brief biography of the candidate",
            "campaign_platform": "Name some campaigns",
            "photo_url": user_data['picture']['large']
        }
    else:
        raise Exception("Error fetching data")


def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }


def insert_voter(conn: Any, voter: dict) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO voters(
            voter_id, 
            voter_name, 
            date_of_birth, 
            gender, 
            nationality, 
            registration_number, 
            address_street,
            address_city,
            address_state,
            address_country,
            address_postcode,
            email,
            phone_number,
            picture,
            registered_age
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            voter['voter_id'],
            voter['voter_name'],
            voter['date_of_birth'],
            voter['gender'],
            voter['nationality'],
            voter['registration_number'],
            voter['address']['street'],
            voter['address']['city'],
            voter['address']['state'],
            voter['address']['country'],
            voter['address']['postcode'],
            voter['email'],
            voter['phone_number'],
            voter['picture'],
            voter['registered_age'],
        )
    )

    conn.commit()


def delivered_report(err, msg):
    if err:
        print(f"Message delivery failed: {msg}")
    else:
        print(f"Message delivery successfully to topic {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":
    pg_conn = SingletonDBConnection(
        host="localhost",
        dbname="voting",
        user="postgres",
        password="postgres",
    ).connect()

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9094'})

    create_tables(pg_conn)

    cursor = pg_conn.cursor()
    cursor.execute(
        """
        SELECT * FROM candidates
        """
    )
    candidates = cursor.fetchall()
    print(candidates)

    if len(candidates) < 3:
        for i in range(3 - len(candidates)):
            candidate = generate_candidates(i, 3)
            print(candidate)

            cursor.execute(
                """
                INSERT INTO candidates(
                    candidate_id, 
                    candidate_name,
                    party_affiliation,
                    biography,
                    campaign_platform,
                    photo_url
                    )
                VALUES(%s,%s,%s,%s,%s,%s)
                """,
                (
                    candidate['candidate_id'],
                    candidate['candidate_name'],
                    candidate['party_affiliation'],
                    candidate['biography'],
                    candidate['campaign_platform'],
                    candidate['photo_url']
                )
            )
            pg_conn.commit()

    for i in range(1000):
        voter_data = generate_voter_data()
        insert_voter(pg_conn, voter_data)
        producer.produce(
            topic="voter_topic",
            key=voter_data['voter_id'],
            value=json.dumps(voter_data),
            on_delivery=delivered_report,
        )
        print(f"Produced voter {i}, data: {voter_data}")
        producer.flush()
