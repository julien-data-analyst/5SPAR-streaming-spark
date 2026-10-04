#################################-
# Project : Streaming Mastodon API historical data based on a hashtag
# Author : Julien RENOULT
# Date : 04/10/2026
#################################-

# Load libraries
from mastodon import Mastodon
from kafka import KafkaProducer
from scripts.utils import log
from datetime import date, datetime

def backfill_tags(
    mastodon: Mastodon,
    producer: KafkaProducer,
    topic: str,
    tags: list[str],
    limit_date: date,
    limit=40, # Add created_date_break 
):
    """Get historical toots by backfilling with given tags to Kafka.

    Args:
        mastodon (Mastodon): Mastodon client to make api calls
        producer (KafkaProducer): Kafka producer to sent data captured
        topic (str): Kafka topic nam to sent data
        tags (list[str]): tag used for filtering the toots
        limit_date (date): limit date to stop the backfill when being inferior to this date
        limit (int, optional): limit of json. Defaults to 40.
    """

    # Get historical data for each given tag
    for tag in tags:
       backfill_tag(mastodon, producer, topic, tag, limit_date, limit)

def backfill_tag( mastodon: Mastodon,
    producer: KafkaProducer,
    topic: str,
    tag: str,
    limit_date: date,
    limit=40, # Add created_date_break 
    ):
        """Get historical toots by backfilling with a given tag to Kafka.

    Args:
        mastodon (Mastodon): Mastodon client to make api calls
        producer (KafkaProducer): Kafka producer to sent data captured
        topic (str): Kafka topic nam to sent data
        tag (str): tag used for filtering the toots
        limit_date (date): limit date to stop the backfill when being inferior to this date
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
                
                #############################-
                # Compare date with the limit to see if we are before the limit
                #############################-
                #print(type(status["created_at"]))
                #print(status["created_at"])
                toot_date = status["created_at"].date()
                #print(f"{toot_date}<{limit_date}")
                
                if toot_date < limit_date:
                    stop=True
                    log(f"Backfill stopped because we stepped before the limit date: {limit_date}")
                    break
                ###############################-
                ###############################-
                
                # Sent the JSON and id to Kafka
                toot_id = str(status["id"])

                message = {
                    "stream_tag" : tag,
                    "value" : status
                }
                producer.send(
                    topic,
                    key=toot_id,
                    value=message,
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