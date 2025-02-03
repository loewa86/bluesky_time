import asyncio
import websockets
import json
import random
import logging
import hashlib
from typing import AsyncGenerator, Any, Dict
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    ExternalId,
    ExternalParentId,
    Url,
    Domain,
)

logging.basicConfig(level=logging.INFO)

DEFAULT_OLDNESS_SECONDS = 3600
DEFAULT_MAXIMUM_ITEMS = 40
DEFAULT_MIN_POST_LENGTH = 5
DEFAULT_SKIP_PROBABILITY = 0.0

def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get(
                "max_oldness_seconds", DEFAULT_OLDNESS_SECONDS
            )
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get(
                "maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS
            )
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH
        
        try:
            skip_probability = parameters.get("skip_probability", DEFAULT_SKIP_PROBABILITY)
        except KeyError:
            skip_probability = DEFAULT_SKIP_PROBABILITY

    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        skip_probability = DEFAULT_SKIP_PROBABILITY

    return (
        max_oldness_seconds,
        maximum_items_to_collect,
        min_post_length,
        skip_probability
    )


# available jetstreams array
jetstreams_array_endpoints = [
    "jetstream1.us-east.bsky.network",
    "jetstream2.us-east.bsky.network",
    "jetstream1.us-west.bsky.network",
    "jetstream2.us-west.bsky.network"
]

async def connect_to_jetstream(max_posts = 50, skip_probability = 0.1):
    selected_jetstream_endpoint = random.choice(jetstreams_array_endpoints) 
    uri = "wss://{}/subscribe?wantedCollections=app.bsky.feed.post".format(selected_jetstream_endpoint)
    logging.info("[BlueSky] Connecting to Jetstream: {}".format(uri))
    post_count = 0
    async with websockets.connect(uri) as websocket:
        logging.info("[BlueSky] Connected to Jetstream WebSocket")
        
        while post_count < max_posts:
            try:
                message = await websocket.recv()
                event = json.loads(message)
                # randomly skip some events
                if random.random() < skip_probability:
                    continue
                
                if not 'commit' in event:
                    continue
                if event['commit']['collection'] == 'app.bsky.feed.post':
                    # or if operation is NOT "create", skip
                    if event['commit']['operation'] != 'create':
                        continue
                    post_count += 1
                    content_ = str(event['commit']['record'].get('text', ''))
                    # author is did
                    author_ = event['did']
                    author_sha1_hex = hashlib.sha1(author_.encode()).hexdigest()
                    created_at = event['commit']['record'].get('createdAt', '')
                    external_id = event['commit']['rkey']
                    # parent id is optional and only exists if the post is a reply, aka has a "reply" field
                    external_parent_id = ""
                    if 'reply' in event['commit']['record']:
                        external_parent_id = event['commit']['record']['reply']['parent']['uri']
                        # just extract the last part of the uri, if /app.bsky.feed.post/ exist, then take what's after
                        if '/app.bsky.feed.post/' in external_parent_id:
                            external_parent_id = external_parent_id.split('/app.bsky.feed.post/')[-1]
                    # the URL is https://bsky.app/profile/did:plc:h6xouawngwws7ozzjpxufyf4/post/3lg5vh2vzis2q
                    # we recompose https://bsky.app/profile/{author_did}/post/{rkey}
                    url =  f"https://bsky.app/profile/{author_}/post/{external_id}"

                    item = Item(
                        # content is the text element of the record  
                        content=Content(content_),
                        # author is the did
                        author=Author(author_sha1_hex),
                        created_at=CreatedAt(created_at),
                        domain=Domain("bsky.app"),
                        external_id=ExternalId(external_id),
                        external_parent_id=ExternalParentId(external_parent_id),
                        url=Url(url)

                    )
                    yield item
                    logging.info("-" * 100)
            except websockets.exceptions.ConnectionClosed:
                logging.info("[BlueSky] Connection closed unexpectedly")
                break
            except json.JSONDecodeError:
                logging.info("[BlueSky] Received invalid JSON")
                continue
            except Exception as e:
                logging.exception(f"[BlueSky] Error in consumer: {e}")

    logging.info("[BlueSky] Finished collecting posts")


async def query(parameters: dict) -> AsyncGenerator[Dict[str, Any], None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length, skip_probability = read_parameters(parameters)
    yielded_items = 0
    
    for _ in range(3):
        if yielded_items >= maximum_items_to_collect:
            break
        
        logging.info(f"[Bluesky] Fetching posts from the firehose, in real time")

        async for item in connect_to_jetstream(max_posts = maximum_items_to_collect, skip_probability = skip_probability):
            if yielded_items >= maximum_items_to_collect:
                break
            if len(item['content']) >= min_post_length:
                yield item
                logging.info(f"[Bluesky] Found post with content: {item}")
                yielded_items += 1
            else:
                logging.info(f"[Bluesky] Post is too short, skipping")
                continue
        # if we retry, sleep 1s
        await asyncio.sleep(1)
    # log how many items were found, and that session is over
    logging.info(f"[Bluesky] Found {yielded_items} items, session is over. Closing connection to Jetstream")    
