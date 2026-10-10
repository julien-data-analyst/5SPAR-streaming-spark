""" Mastodon producer - Get streaming or historical data from the Mastodon API. 

The producer supports two execution modes: 
- ``stream``: continuously consume new toots from Mastodon streams. 
- ``backfill``: retrieve historical toots up to a given date. 

Environment variables: 
KAFKA_BOOTSTRAP: Kafka bootstrap server address. Defaults to ``kafka:9092``. 
TOPIC: Kafka topic used to publish Mastodon toots. Defaults to ``mastodon_stream``. 
MASTODON_URL: Mastodon instance URL. Defaults to ``https://mastodon.social``. 
MASTODON_ACCESS_TOKEN: Access token used to authenticate against the Mastodon API. 
This variable is required. MASTODON_TAGS: Comma-separated list of hashtags to monitor. 
Defaults to ``datascience``. 
Kafka messages: key: Mastodon toot ID, encoded as a UTF-8 string. value: JSON representation of the toot contents. 
The toot ID is used as the Kafka message key to preserve ordering and facilitate the identification of new records. 
"""

import json
import os
import time
from scripts.utils import log
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import argparse
from mastodon import Mastodon
from scripts.backfill import backfill_tags
from scripts.streaming import stream_tags
from datetime import date

#######################################-
# Config : get en vars for streaming data
#######################################-

# Kafka config
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "mastodon_stream")

# Mastodon config
MASTODON_URL = os.environ.get(
    "MASTODON_URL",
    "https://mastodon.social",
)

MASTODON_ACCESS_TOKEN = os.environ["MASTODON_ACCESS_TOKEN"]

MASTODON_TAGS = [
    tag.strip()
    for tag in os.environ.get("MASTODON_TAGS", "datscience").split(",")
    if tag.strip()
]

## Get args passed for mode streaming or backfill
def parse_args():
    """Parse and validate command-line arguments. 
    
    The producer can operate in either streaming or backfill mode. 
    The ``--until`` argument defines the maximum date to retrieve when running in backfill mode. 
    
    Returns: 
        argparse.Namespace: 
            Parsed command-line arguments containing: 
                - ``mode``: Producer execution mode (``stream`` or ``backfill``). 
                - ``until``: Maximum date used for backfill. 
                
    Raises: SystemExit: 
        If the provided command-line arguments are invalid. 
    """
    
    parser = argparse.ArgumentParser(
        description="Producer Mastodon to Kafka"
    )

    parser.add_argument(
        "--mode",
        choices=["stream", "backfill"],
        required=True,
        help="Producer execution mode",
    )

    parser.add_argument(
        "--until",
        type=date.fromisoformat,
        default=date(2026, 1, 1),
        help=(
            "Limit date in format YYYY-MM-DD "
        ),
    )
    
    return parser.parse_args()

####################################################
####################################################
# tags = [text.strip() for text in os.getenv("MASTODON_TAGS").split(",")]
# print("Tags list to use for streaming:", tags)

# For backfill define a date min for stopping backfill (created_date : 2026-01-01T00:00:00.000Z)
# For streaming use of maximum id conserved when doing a backfill or streaming (env vars to modify)
def serialize_value(value):
    """Sent the JSON value of the results if we can, else it returns None and ignore the JSON

    Args:
        value (str): JSON string to dump

    Returns:
        dict: return the JSON converted into python dictionary, else returns None
    """
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")
    except Exception:
        print("Serialization failed")
        print("Object type:", type(value))
        return None
        # print("Object:", value)
        #raise
    
def main():
    """Run the Mastodon producer. 
    
    This function initializes the Kafka administrator client, waits for Kafka and the target topic to become available, creates the Kafka producer, 
    connects to the Mastodon API, and starts the requested producer mode. 
    
    In ``backfill`` mode, historical toots are retrieved until the date specified by ``--until``. 
    In ``stream`` mode, the producer continuously consumes new toots from the configured Mastodon hashtags. 
    
    The function also handles graceful shutdown of the Kafka producer when the streaming process is interrupted. 
    
    Raises: 
        KeyError: If ``MASTODON_ACCESS_TOKEN`` is not defined. 
        KeyboardInterrupt: If the producer is interrupted during startup or execution. 
    """
    
    args = parse_args()
    
    # 1) Wait for kafka to be available
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="mastodon-admin")
            break
        except NoBrokersAvailable:
            log(f"Kafka not available on {BOOTSTRAP}, retry in 3s...")
            time.sleep(3)

    # 2) Wait for the topic to be created 
    while TOPIC not in admin.list_topics():
        log(f"Topic '{TOPIC}' doesn't exists yet. Create then. retry in 3s...")
        time.sleep(5)
    admin.close()

    # 3) Create the kafka producer to get the data captured
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=serialize_value,
        linger_ms=50,
    )

    # 3) Connecting to the Mastodon API using a token specified in the Docker .env file
    mastodon = Mastodon(
        access_token=MASTODON_ACCESS_TOKEN,
        api_base_url=MASTODON_URL,
    )

    # 4) Account verified credentials
    account = mastodon.account_verify_credentials()

    log(f"Mastodon connected with {account['username']}")


    if args.mode == "backfill":
        backfill_tags(
            mastodon,
            producer,
            TOPIC,
            MASTODON_TAGS,
            args.until
        )

    elif args.mode == "stream":
        try:
            stream_tags(
                mastodon,
                producer,
                TOPIC,
                MASTODON_TAGS,
            )

            # Keep the main process alive while the
            # asynchronous Mastodon streams are running.
            while True:
                time.sleep(60)

        except KeyboardInterrupt:
            log("Stopping Mastodon streaming...")

        finally:
            producer.flush()
            producer.close()
            log("Producer stopped.")
        # stream_tag(mastodon, producer, TOPIC, tag=MASTODON_TAGS[0])
            

if __name__ == "__main__":
    main()


