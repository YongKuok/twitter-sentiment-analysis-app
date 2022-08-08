# Tweepy streamer
from tweepy import OAuthHandler
from tweepy import StreamingClient
from tweepy import StreamRule

# kafka broker
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

# Regular Expression
import re

import requests
import configparser
import json
from datetime import datetime

parser = configparser.ConfigParser(interpolation=None)
parser.read("credentials.conf")
bearer_token = parser.get("tweepy_config", "bearer_token")
kafka_topic_name = parser.get("kafka_config", "kafka_topic_name")
kafka_bootstrap_servers = parser.get("kafka_config", "kafka_bootstrap_servers")

##### TWITTER STREAM #####
class TwitterStreamer(StreamingClient):
    def __init__(self, bearer_token,kafka_producer = None, topic_name=None):
        super().__init__(bearer_token)
        self.kafka_producer = kafka_producer
        self.topic_name = topic_name
        
    def start_stream(self):
        # Start stream
        self.filter(expansions="referenced_tweets.id", tweet_fields= ["created_at"])
    
    def add_rules(self, add, **params):
        return super().add_rules(add, **params)

    def delete_rules(self, ids, **params):
        return super().delete_rules(ids, **params)

    def create_rule(self, keyword):
        rule_value = re.sub(r"\s+", " OR ", keyword)
        rule_value = rule_value + " lang:en"
        rule = StreamRule(value = rule_value, tag=keyword)
        return rule

    def delete_all_rules(self):
        listrules = self.get_rules()
        if listrules[0] != None:
            for i in listrules[0]:
                self.delete_rules(i.id)
        return self.get_rules()[0]
        

    def on_data(self, data):
        try:
            data = json.loads(data)
            msg = {}
            msg["tweet_id"] = data["data"]["id"]
            msg["creation_timestamp"] = data["data"]["created_at"]
            text = ""
            if "referenced_tweets" in data["data"] and data["data"]["referenced_tweets"][0]["type"] == "retweeted":
                text = data["includes"]["tweets"][0]["text"]
            else:
                text = data["data"]["text"]
            msg["text"] = text

            keyword_list = list(map(lambda x: x["tag"], data["matching_rules"]))            
            for keyword in keyword_list:
                msg["keyword"] = keyword
                msg_str = json.dumps(msg)
                print(msg['creation_timestamp'])
                producer.send(self.topic_name, msg_str.encode("utf-8"))
            return True
        
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True
    
    def on_error(self, error):
        print("Error on stream: {}".format(error))
        return True
    
    def on_connection_error(self):
        print("Stream connection error/timeout.")
        return True


if __name__ == "__main__":
    admin = KafkaAdminClient(bootstrap_servers=[kafka_bootstrap_servers], client_id = "client1")
    kafka_topic = NewTopic(name=kafka_topic_name, num_partitions=1, replication_factor=1)

    try:
        admin.create_topics([kafka_topic])
    except Exception as err:
        print("Error on create kafka topic:" + err.message)

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    twitter_stream = TwitterStreamer(
        bearer_token,
        kafka_producer=producer, 
        topic_name=kafka_topic_name
        )
    twitter_stream.start_stream()

    



