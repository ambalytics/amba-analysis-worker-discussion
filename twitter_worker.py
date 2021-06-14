import json
import socket
import time
import doi_resolver

from kafka import KafkaConsumer, KafkaProducer
import pymongo

topic_name = 'tweet_queue'

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


# may want this extra
def find_doi(urls):
    # error handling
    # check for doi in expanded url
    # check for doi in unwound url
    # check for doi in meta of request response
    # check for doi using search ambalytics
    return 'doi'


def startWorker():
    print('Running Consumer..')

    consumer = KafkaConsumer(topic_name, group_id='worker',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    mongoClient = pymongo.MongoClient("mongodb://localhost:27017/")
    db = mongoClient["TweetTest"]

    link = db["test_link"]
    no_link = db["test_no_link"]
    no_url = db["test_no_url"]

    # for msg in consumer:
    #     print(msg.value)
    while True:
        try:
            for msg in consumer:
                # print(msg.value)
                json_response = json.loads(msg.value)
                if 'id' in json_response['data']:
                    print(json_response['data']['id'])
                    json_response['data']['_id'] = json_response['data'].pop('id')
                    json_response['data']['matching_rules'] = json_response['matching_rules']
                    if 'entities' in json_response['data']:
                        if 'urls' in json_response['data']['entities']:
                            doi = False
                            for url in json_response['data']['entities']['urls']:
                                if doi is False and 'expanded_url' in url:
                                    doi = doi_resolver.link_url(url['expanded_url'])
                                if doi is False and 'unwound_url' in url:
                                    doi_resolver.link_url(url['unwound_url'])
                            if doi is not False:
                                json_response['data']['doi'] = doi
                                link.insert_one(json_response['data'])
                            else:
                                no_link.insert_one(json_response['data'])
                        # else:
                            # retweets
                            # no_url.insert_one(json_response['data'])
                    # print(json.dumps(json_response['data']['id'], indent=4, sort_keys=True))
                    # print(json.dumps(json_response['data']['_id'], indent=4, sort_keys=True))
                    # x = includes.insert_one(json_response['data'])

        except pymongo.errors.DuplicateKeyError:
            print('found')
        except:
            consumer.close()
            print("Goodbye")
            break

# this is only relevant if it consume -> produce
    # consumer.close()
    # sleep(5)
    #
    # if len(parsed_records) > 0:
    #     print('Publishing records..')
    #     producer = connect_kafka_producer()
    #     for rec in parsed_records:
    #         publish_message(producer, parsed_topic_name, 'parsed', rec)

if __name__ == '__main__':
    startWorker()
