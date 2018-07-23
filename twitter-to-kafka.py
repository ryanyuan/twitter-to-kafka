#!/usr/bin/env python

"""This script uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and publish them to a Kafka topic.
"""

import base64
import datetime
import os
import logging
import ConfigParser
import simplejson as json
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer

# Get your twitter credentials from the environment variables.
# These are set in the 'twitter-stream.json' manifest file.

config = ConfigParser.RawConfigParser()
config.read('config.cfg')

CONSUMER_KEY = config.get('Twitter', 'consumer_key')
CONSUMER_SECRET = config.get('Twitter', 'consumer_secret')
ACCESS_TOKEN = config.get('Twitter', 'access_token')
ACCESS_TOKEN_SECRET = config.get('Twitter', 'access_token_secret')
TWITTER_STREAMING_MODE = config.get('Twitter', 'streaming_mode')
KAFKA_ENDPOINT = '{0}:{1}'.format(config.get('Kafka', 'kafka_endpoint'), config.get('Kafka', 'kafka_endpoint_port'))
KAFKA_TOPIC = config.get('Kafka', 'topic')
NUM_RETRIES = 3

class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a Kafka topic
    """

    producer = KafkaProducer(bootstrap_servers=KAFKA_ENDPOINT)

    def on_data(self, data):
        """What to do when tweet data is received."""
        data_json = json.loads(data)
        str_tweet = data_json['text'].encode('utf-8')
        self.producer.send(KAFKA_TOPIC, str_tweet)
        print(str_tweet)

    def on_error(self, status):
        print status


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )

    listener = StdOutListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    print 'stream mode is: %s' % TWITTER_STREAMING_MODE

    stream = Stream(auth, listener)
    # set up the streaming depending upon whether our mode is 'sample', which
    # will sample the twitter public stream. If not 'sample', instead track
    # the given set of keywords.
    # This environment var is set in the 'twitter-stream.yaml' file.
    if TWITTER_STREAMING_MODE == 'sample':
        stream.sample()
    else:
        stream.filter(track=['google'])