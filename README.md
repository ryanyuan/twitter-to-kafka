# Streaming Twitter's tweets to Apache Kafka

This project enables tweets streaming process from [Twitter](https://developer.twitter.com/en/docs/developer-utilities/twitter-libraries.html) to [Kafka](http://kafka.apache.org/).

# Prerequisite

You need to have a running Kafka service.

# Environment configuartions and validations

Open `config.cfg` and edit the values using text editor:

```
$ vim config.cfg
```

or 

```
$ nano config.cfg
```

Now let's check if `Producer` and `Consumer` are able to send and receive messages.

```
$ python kafka-test.py
...
ConsumerRecord(topic=u'mytopic', partition=1, offset=2244, timestamp=1532095366718, timestamp_type=0, key=None, value='test', checksum=None, serialized_key_size=-1, serialized_value_size=4)
ConsumerRecord(topic=u'mytopic', partition=1, offset=2245, timestamp=1532095366718, timestamp_type=0, key=None, value='\xc2Hola, mundo!', checksum=None, serialized_key_size=-1, serialized_value_size=13)
...
```

# Step-by-step

Run let's run the script to stream the tweets from Twitter to Apache Kafka.

```
$ python twitter-to-kafka.py
stream mode is: normal
RT @ryan190512: When you search ‘top gear audience’ on google images https://t.co/fbwPkr9yVo
RT @wkrakower: Education Chat Calendar: https://t.co/YYpWq0BViq #EdCampLdrNY as @donald_gately and @dmgately are discussing.
RT @speezbenchmark: Go time! New episode of the podcast is up. Check it out and spread the good word! #NXT #G1Climax #NJPW
...
```

Congrats! Now the Kafka can stream the realtime tweets from twitter.