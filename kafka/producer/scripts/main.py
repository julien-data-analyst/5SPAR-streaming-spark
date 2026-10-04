"""
MastodonStream - get streaming/historical data from the mastodon API

envars :
  KAFKA_BOOTSTRAP  (default kafka:9092)
  TOPIC            (défaut reviews_stream)

each message :
  key   = id (ensures the order of tweets based on this ID and uses it to filter new tweets)
  value = JSON of the toots contents
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
from scripts.streaming import stream_tag
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
def main():
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
        value_serializer=lambda v: json.dumps(
            v,
            ensure_ascii=False,
            default=str,
        ).encode("utf-8"),
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
        stream_tag(
            mastodon,
            producer,
            TOPIC,
            MASTODON_TAGS,
        )


if __name__ == "__main__":
    main()


