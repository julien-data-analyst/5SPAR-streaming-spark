#################################-
# Project : Streaming Mastodon API historical data based on a hashtag
# Author : Julien RENOULT
# Date : 04/10/2026
#################################-

# Load libraries
from mastodon import Mastodon
from kafka import KafkaProducer
from scripts.utils import log

def backfill_tags(
    mastodon: Mastodon,
    producer: KafkaProducer,
    topic: str,
    tags: list[str],
    limit=40,
):
    """Get historical toots by backfilling with a given tag to Kafka.

    Args:
        mastodon (Mastodon): Mastodon client to make api calls
        producer (KafkaProducer): Kafka producer to sent data captured
        topic (str): Kafka topic nam to sent data
        tag (str): tag used for filtering the toots
        limit (int, optional): limit of json. Defaults to 40.
    """

    # Get historical data for each given tag
    for tag in tags:
       backfill_tag(mastodon, producer, topic, tag, limit)

def backfill_tag( mastodon: Mastodon,
    producer: KafkaProducer,
    topic: str,
    tag: str,
    limit=40):
        """Get historical toots by backfilling with a given tag to Kafka.

    Args:
        mastodon (Mastodon): Mastodon client to make api calls
        producer (KafkaProducer): Kafka producer to sent data captured
        topic (str): Kafka topic nam to sent data
        tag (str): tag used for filtering the toots
        limit (int, optional): limit of json. Defaults to 40.
        """
        
        max_id = None
        sent = 0
        log(f"Backfill begins for #{tag}")
        
        # Continue until we don't have any toots anymore
        while True:
            
            # API REST calls to get toots based on the given id
            statuses = mastodon.timeline_hashtag(
                tag,
                limit=limit,
                max_id=max_id,
            )

            # If None, then we know that it's the end of the backfill breaking the while
            if not statuses:
                log(f"Fin du backfill pour #{tag}")
                break
            
            # If data then send the data for each toot with the toot_id as key message
            for status in statuses:
                toot_id = str(status["id"])

                producer.send(
                    topic,
                    key=toot_id,
                    value=status,
                )

                sent += 1

            # The last element is the older toots for the result so 
            # we will get the id to used for the next call
            max_id = str(statuses[-1]["id"])

            log(
                f"Backfill #{tag}: "
                f"{len(statuses)} toots sent "
                f"(total={sent})"
            )

        producer.flush()

        log(
            f"Backfill finished for #{tag}: "
            f"{sent} toots sent to Kafka in total"
        )