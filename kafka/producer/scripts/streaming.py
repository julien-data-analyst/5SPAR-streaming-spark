#################################
# Project : Streaming Mastodon API in real time based on a hashtag
# Author : Julien RENOULT
# Date : 04/10/2026
#################################

import time
from typing import Callable

from mastodon import Mastodon, CallbackStreamListener
from kafka import KafkaProducer

from scripts.utils import log


def make_update_callback(producer: KafkaProducer, topic: str, tag: str) -> Callable:
    """Creates the callback that is called whenever a new toot is posted with a given tag.

    Args:
        producer (KafkaProducer): Producer Kafka to sent the data captured
        topic (str): Topic name Kafka to sent the data
        tag (str): tag used for filtering toots

    Returns:
        Callable: _description_
    """
    sent = 0  # tag counter

    def on_update(status):
        nonlocal sent
        
        # Prepare message
        toot_id = str(status["id"])
        message = {
            "stream_tag": tag,
            "status": status,
        }
        
        producer.send(topic, key=toot_id, value=message)
        sent += 1
        
        log(f"[#{tag}] Toot {toot_id} sent to Kafka (total={sent})")

    return on_update


def build_listener(producer: KafkaProducer, topic: str, tag: str) -> CallbackStreamListener:
    """Construit un listener basé sur callback pour un tag."""
    return CallbackStreamListener(
        local_update_handler=make_update_callback(producer, topic, tag),
    )


def stream_tags(mastodon: Mastodon, producer: KafkaProducer, topic: str, tags: list[str]):
    """Monitor multiple hashtags at the same time (one thread per hashtag).

    Args:
        mastodon (Mastodon): Mastodon client for making calls to the API
        producer (KafkaProducer): Producer Kafka to sent the data captured
        topic (str): Topic name Kafka to sent the data
        tags (list[str]): tags used for filtering toots (one thread by tag)

    Returns:
        handles: handles list of handle foe each tag
    """
    
    handles = []
    for tag in tags:
        log(f"Streaming toots begins for tag: #{tag}")
        handle = mastodon.stream_hashtag(
            tag,
            build_listener(producer, topic, tag),
            run_async=True,
            reconnect_async=True,  # reconnexion automatique en cas de coupure
            reconnect_async_wait_sec=10,
        )
        handles.append(handle)
    
    return handles  # handle.close() pour arrêter proprement


def stream_tag(mastodon: Mastodon, producer: KafkaProducer, topic: str, tag: str):
    """Monitor one tag which continually connect 

    Args:
        mastodon (Mastodon): Mastodon client for making calls to the API
        producer (KafkaProducer): Producer Kafka to sent the data captured
        topic (str): Topic name Kafka to sent the data
        tag (str): tag used for filtering toots
        
    """
    while True:
        try:
            # Activate a stream on the given tag
            log(f"Starting Mastodon stream for #{tag}")
            mastodon.stream_hashtag(
                tag,
                build_listener(producer, topic, tag),
                run_async=False,
            )
            
        except KeyboardInterrupt:
            log(f"Stopping Mastodon stream for #{tag}")
            break
        
        except Exception as err:
            log(f"Mastodon stream for #{tag} disconnected: {err}. Retrying in 10 seconds...")
            time.sleep(10)