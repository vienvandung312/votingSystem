import json

import pandas as pd
import streamlit as st
import time
from db import SingletonDBConnection
from confluent_kafka import Consumer


def fetch_voting_stats(conn):
    cur = conn.cursor()

    cur.execute(
        """
        SELECT count(*) FROM voters
        """
    )
    voter_count = cur.fetchone()[0]

    cur.execute(
        """
        SELECT count(*) from candidates
        """
    )

    candidate_count = cur.fetchone()[0]

    return voter_count, candidate_count


def create_kafka_consumer(topic_name):
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9094",
            "group.id": "voting-group",
            "auto.offset.reset": "earliest"
        }
    )
    consumer.subscribe(topic_name)
    return consumer


def fetch_data_from_kafka(consumer):
    messages = consumer.poll(1)
    data = []
    for message in json.loads(messages.value().decode("utf-8").strip):
        for sub_message in message:
            data.append(sub_message.value())
    return data


def update_data(conn):
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    voter_cnt, candidate_cnt = fetch_voting_stats(conn)

    st.markdown("""---""")

    col1, col2 = st.columns(2)
    col1.metric("Total voters", voter_cnt)
    col2.metric("Total candidates", candidate_cnt)
    consumer = create_kafka_consumer(["aggregated_votes_per_candidates"])
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)

    results = results.loc[results.groupby("candidate_id")['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    st.markdown("""---""")
    st.header("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=20)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader("Total votes: {}".format(leading_candidate['total_votes']))


conn = SingletonDBConnection(
    host="localhost",
    dbname="voting",
    user="postgres",
    password="postgres",
).connect()

st.title("Realtime Election Voting Dashboard")

update_data(conn)
