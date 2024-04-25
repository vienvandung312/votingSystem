import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import sum as spark_sum, col, from_json
import logging


if __name__ == "__main__":
    logging.info(f"You're using PySpark {pyspark.__version__}")
    logging.info(f"Starting SparkSession")
    spark = (
        SparkSession
        .builder
        .appName("VotingSystem")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.adaptive.enable", "false")
    ).getOrCreate()

    vote_schema = StructType([
        StructField('voter_id', StringType()),
        StructField('candidate_id', StringType()),
        StructField('voting_time', TimestampType()),
        StructField('voter_name', StringType()),
        StructField('party_affiliation', StringType()),
        StructField('biography', StringType()),
        StructField('campaign_platform', StringType()),
        StructField('photo_url', StringType()),
        StructField('candidate_name', StringType()),
        StructField('date_of_birth', StringType()),
        StructField('gender', StringType()),
        StructField('nationality', StringType()),
        StructField('registration_number', StringType()),
        StructField('address', StructType([
            StructField('street', StringType()),
            StructField('city', StringType()),
            StructField('state', StringType()),
            StructField('country', StringType()),
            StructField('postcode', StringType())
        ])),
        StructField('phone_number', StringType()),
        StructField('picture', StringType()),
        StructField('registered_age', IntegerType()),
        StructField('vote', IntegerType()),
    ])

    vote_df = (
        spark
        .readStream
        .format('kafka')
        .option("kafka.bootstrap.servers", "localhost:9094")
        .option("subscribe", "votes_topic")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value as STRING)")
        .select(from_json(col=col('value'), schema=vote_schema).alias('data'))
        .select('data.*')
    )

    vote_df = (
        vote_df
        .withColumn(colName='voting_time', col=col('voting_time').cast(TimestampType()))
        .withColumn(colName='vote', col=col('vote').cast(IntegerType()))
    )

    enriched_votes_df = (
        vote_df
        .withWatermark(eventTime='voting_time', delayThreshold="1 minute")
    )

    votes_per_candidate = (
        enriched_votes_df
        .groupBy(['candidate_id', 'candidate_name', 'party_affiliation', 'photo_url'])
        .agg(
            spark_sum('vote').alias('total_votes')
        )
    )

    turnout_by_location = (
        enriched_votes_df
        .groupBy('address.state')
        .count()
        .alias('total_votes')
    )

    votes_per_candidate_to_kafka = (
        votes_per_candidate
        .selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format('kafka')
        .option("kafka.bootstrap.servers", "localhost:9094")
        .option("topic", "aggregated_votes_per_candidates")
        .option("checkpointLocation", "checkpoints/votes_per_candidate")
        .outputMode('update')
        .start()
    )

    turnout_by_location_to_kafka = (
        turnout_by_location
        .selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format('kafka')
        .option("kafka.bootstrap.servers", "localhost:9094")
        .option("topic", "aggregated_turnout_by_location")
        .option("checkpointLocation", "checkpoints/turnout_by_location")
        .outputMode('update')
        .start()
    )

    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()
