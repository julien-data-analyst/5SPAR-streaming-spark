#################################-
# Project : Streaming Mastodon API in real time based on a hashtag
# Author : Julien RENOULT
# Date : 04/10/2026
#################################-

from mastodon import StreamListener,Mastodon
from scripts.utils import log
from kafka import KafkaProducer

# 4) Listener Mastodon qui va capturer les tweets au topic donné de Kafka
class MastodonListener(StreamListener):

    def __init__(self, producer: KafkaProducer, topic: str, tag: str):
        """generate a mastodon stream listener with a kafka producer and topic

        Args:
            producer (KafkaProducer): Kafka producer to sent data captured
            topic (str): Topic kafka of the producer to sent data
        """
        super().__init__()
        self.sent = 0
        self.producer = producer
        self.topic = topic
        self.tag = tag
        
    def on_update(self, status):
        """
        Automatically called when new toots is called
        """


        toot_id = str(status["id"])
        message = {
        "stream_tag": self.tag,
        "status": status,
        }
        
        self.producer.send(
            self.topic,
            key=toot_id,
            value=message,
        )

        self.sent += 1

        log(
            f"Toot {toot_id} sent at Kafka "
            f"(total={self.sent})"
        )

    def on_abort(self, err):
        """
        When interrupting a Mastodon streaming
        """
        log(f"Interrupt mastodon streaming: {err}")
        # Register last id in a json format into folder last_toot/...

def stream_tag(mastodon: Mastodon, producer: KafkaProducer, topic: str, tags: list[str], limit: int=20):
    """Listen in real time the toots associated with a hashtag

    Args:
        mastodon (Mastodon): Mastodon client for making calls to the API
        producer (KafkaProducer): Producer Kafka to sent the data captured
        topic (str): Topic name Kafka to sent the data
        tag (str): tag used for filtering toots
        limit (int) : json toots limit by api call
    """

    
    for tag in tags:
        log(f"Streaming toots begins for tag: #{tag}")
        listener = MastodonListener(producer, topic, tag)
        mastodon.stream_hashtag(
            tag,
            listener,
            run_async=True, # Many tags to watch out
        )