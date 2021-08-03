import logging
import math
import re
from datetime import date, datetime
from multiprocessing.context import Process
from difflib import SequenceMatcher
import spacy
from collections import Counter

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


# todo heartbeat kafka?
# WARNING:kafka.coordinator:Heartbeat failed for group twitter-worker because it is rebalancing
# WARNING:kafka.coordinator:Heartbeat failed ([Error 27] RebalanceInProgressError); retrying

# Process ForkPoolWorker-2:1:
# Traceback (most recent call last):
# File "/usr/local/lib/python3.6/multiprocessing/process.py", line 258, in _bootstrap
# self.run()
# File "/usr/local/lib/python3.6/multiprocessing/process.py", line 93, in run
# self._target(*self._args, **self._kwargs)
# File "/usr/local/lib/python3.6/multiprocessing/pool.py", line 103, in worker
# initializer(*initargs)
# File "/usr/local/lib/python3.6/site-packages/event_stream/event_stream_consumer.py", line 109, in worker
# self.on_message(item)
# File "/src/src/twitter_worker.py", line 93, in on_message
# e.data['obj']['data']['abstract'])
# File "/src/src/twitter_worker.py", line 16, in compare_text
# return SequenceMatcher(None, a, b).ratio()
# File "/usr/local/lib/python3.6/difflib.py", line 644, in ratio
# matches = sum(triple[-1] for triple in self.get_matching_blocks())
# File "/usr/local/lib/python3.6/difflib.py", line 467, in get_matching_blocks
# la, lb = len(self.a), len(self.b)
# TypeError: object of type 'NoneType' has no len()

# compare text and return score
def compare_text(a, b):
    if not a or not b:
        return 0
    return SequenceMatcher(None, a, b).ratio()


def normalize(string):
    # todo numbers, special characters/languages
    return (re.sub('[^a-zA-Z ]+', '', string)).casefold().strip()


def score_time(x):
    y = (math.log(x) / math.log(1 / 7) + 2) * 10
    if y > 30:
        return 30
    if y < 1:
        return 1
    return y


def score_type(type):
    if type == 'quote':
        return 7
    if type == 'retweet':
        return 6
    # tweet
    return 10


class TwitterWorker(EventStreamConsumer, EventStreamProducer):
    state = "linked"
    relation_type = "discusses"
    publication_client = False
    log = "TwitterWorker "
    group_id = "twitter-worker"
    nlp = False

    source_score = {
        'Twitter Web App': 10,
        'Twitter for iPhone': 10,
        'Twitter Web Android': 10,
        'Twitter Web iPad': 10,
        'Twitter Web Mac': 10,
        'TweetDeck': 5,
        'Twitter': 3,
    }

    def on_message(self, json_msg):
        # print('hello')
        # print(json_msg)
        logging.warning(self.log + "on message twitter consumer")

        e = Event()
        e.from_json(json_msg)

        e.data['subj']['processed'] = {}
        e.data['subj']['processed']['question_mark_count'] = e.data['subj']['data']['text'].count("?")
        e.data['subj']['processed']['exclamation_mark_count'] = e.data['subj']['data']['text'].count("!")
        e.data['subj']['processed']['length'] = len(e.data['subj']['data']['text'])

        split_date = e.data['obj']['data']['pubDate'].split('-')
        pub_timestamp = date(int(split_date[0]), int(split_date[1]), int(split_date[2]))

        # todo use date from twitter not today
        e.data['subj']['processed']['time_past'] = (date.today() - pub_timestamp).days

        hashtags = []
        annotations = []
        a_types = []

        if 'entities' in e.data['subj']['data']:
            if 'hashtags' in e.data['subj']['data']['entities']:
                for tag in e.data['subj']['data']['entities']['hashtags']:
                    hashtags.append(normalize(tag['tag']))

            # todo filter annotation types
            if 'annotations' in e.data['subj']['data']['entities']:
                for tag in e.data['subj']['data']['entities']['annotations']:
                    annotations.append(tag['normalized_text'])
                    a_types.append(tag['type'])

        e.data['subj']['processed']['hashtags'] = hashtags
        e.data['subj']['processed']['annotations'] = annotations
        e.data['subj']['processed']['a_types'] = a_types

        # todo filter context annotation types
        context_a_domain = []
        context_a_entity = []
        if 'context_annotations' in e.data['subj']['data']:
            for tag in e.data['subj']['data']['context_annotations']:
                # context_a_domain.append(tag['name'])
                # context_a_entity.append(tag['name'])
                logging.warning('context a domain append tag name %s' % tag)
        e.data['subj']['processed']['context_domain'] = context_a_domain
        e.data['subj']['processed']['context_entity'] = context_a_entity

        # containsAbstract, calculate score for comparison
        e.data['subj']['processed']['contains_abstract'] = compare_text(e.data['subj']['data']['text'],
                                                                        e.data['obj']['data']['abstract'])
        # isBot
        if e.data['subj']['data']['source'] in self.source_score:
            e.data['subj']['processed']['bot_rating'] = self.source_score[e.data['subj']['data']['source']]
        else:
            e.data['subj']['processed']['bot_rating'] = 1

        # typeOfTweet (quote, retweet, tweet)
        if not e.data['subj']['data']['referenced_tweets'][0]['type']:
            e.data['subj']['processed']['tweet_type'] = 'tweet'
        else:
            e.data['subj']['processed']['tweet_type'] = e.data['subj']['data']['referenced_tweets'][0]['type']

        # use nltk and check for lang = eng, stop words etc
        # normalized tokenized top words from text, remove hashtags and stuff ?
        text = e.data['subj']['data']['text'].strip().lower()
        e.data['subj']['processed']['words'] = self.spacy_process(text)

        # - Sentiment https://realpython.com/sentiment-analysis-python/
        # - score (how high we rank this tweet)
        # - match tweet author and publication author # compare just with text compare

        bot_score = e.data['subj']['processed']['bot_rating']
        type_score = score_type(e.data['subj']['processed']['tweet_type'])
        dt = datetime(int(split_date[0]), int(split_date[1]), int(split_date[2]))
        time_score = score_time((datetime.today() - dt).days)
        abstract_score = (2 / (e.data['subj']['processed']['contains_abstract'] + 1) - 1) * 10  # todo check
        logging.warning('score %s - %s - %s - %s' % (time_score, type_score, bot_score, abstract_score))
        e.data['subj']['processed']['score'] = time_score * type_score * bot_score * abstract_score

        e.set('state', 'processed')
        self.publish(e)

    # https://towardsdatascience.com/text-normalization-with-spacy-and-nltk-1302ff430119
    # todo remove hashtags? remove @somebody
    def spacy_process(self, text):
        if not self.nlp:
            self.nlp = spacy.load('en_core_web_sm')

        doc = self.nlp(text)

        # Tokenization and lemmatization are done with the spacy nlp pipeline commands
        lemma_list = []
        for token in doc:
            lemma_list.append(token.lemma_)
        # print("Tokenize+Lemmatize:")
        # print(lemma_list)

        # Filter the stopword
        filtered_sentence = []
        for word in lemma_list:
            lexeme = self.nlp.vocab[word]
            if lexeme.is_stop == False:
                filtered_sentence.append(word)

                # Remove punctuation
        punctuations = "?:!.,;"
        for word in filtered_sentence:
            if word in punctuations:
                filtered_sentence.remove(word)
        # print(" ")
        # print("Remove stopword & punctuation: ")
        # print(filtered_sentence)

        # remove count since it is just short?
        word_freq = Counter(filtered_sentence)
        common_words = word_freq.most_common(10)
        return common_words
# from kafka import KafkaConsumer, KafkaProducer
#
# topic_name = 'events'
# global running
# running = True


# import logging
# logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')
# logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

# def publish_message(producer_instance, topic_name, key, value):
#     try:
#         key_bytes = bytes(key, encoding='utf-8')
#         value_bytes = value
#         producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
#         producer_instance.flush()
#         logging.warning('Message published successfully.')
#     except Exception as ex:
#         logging.warning('Exception in publishing message')
#         print(str(ex))
#
#
# def connect_kafka_producer():
#     _producer = None
#     try:
#         _producer = KafkaProducer(bootstrap_servers=['kafka:9092'], api_version=(0, 10))
#     except Exception as ex:
#         logging.warning('Exception while connecting Kafka')
#         logging.warning(str(ex))
#     finally:
#         return _producer
#


# def stop():
#     global running
#     running = False
#
#
# def start_worker(worker_id):
#     global running
#     running = True
#     logging.warning('Starting Consumer.. ' + str(worker_id))
#     # todo config
#     consumer = KafkaConsumer(topic_name, group_id='worker',
#                              bootstrap_servers=['kafka:9092'], api_version=(0, 10), consumer_timeout_ms=5000)
#
#     producer = connect_kafka_producer()
#     parsed_topic_name = 'linked'
#
#     while running:
#         try:
#             for msg in consumer:
#                 # print(msg.value)
#                 json_response = json.loads(msg.value)
#                 if 'id' in json_response['data']:
#                     logging.warning(str(worker_id) + " " + json_response['data']['id'])
#
#                     # we use the id for mongo
#                     json_response['data']['_id'] = json_response['data'].pop('id')
#                     # move matching rules to tweet self
#                     json_response['data']['matching_rules'] = json_response['matching_rules']
#                     running = False
#                     # check for doi recognition on tweet self
#                     doi = url_doi_check(json_response['data'])
#                     logging.warning('doi 1 ' + str(doi))
#                     if doi is not False:
#                         json_response['data']['doi'] = doi
#                         logging.warning(str(worker_id) + " " + json_response['data']['_id'] + " doi self")
#                         publish_message(producer, parsed_topic_name, 'parsed', json.dumps(json_response['data'], indent=2).encode('utf-8'))
#                         # check the includes object for the original tweet url
#                     elif 'tweets' in json_response['includes']:
#                         logging.warning('tweets')
#                         for tweet in json_response['includes']['tweets']:
#                             doi = url_doi_check(tweet)
#                             logging.warning('doi 2 ' + str(doi))
#                             if doi is not False:
#                                 # use first doi we get
#                                 logging.warning(str(worker_id) + " " + json_response['data']['_id'] + " doi includes")
#                                 json_response['data']['doi'] = doi
#                                 publish_message(producer, parsed_topic_name, 'parsed', json.dumps(json_response['data'], indent=2).encode('utf-8'))
#                                 break
#                     else:
#                         logging.warning(str(worker_id) + " " + json_response['data']['id'] + " no doi")
#                         # no_link.insert_one(json_response['data'])
#         # todo thread queue working on messages, on submit return the result and publish (async)
#
#         except Exception as exc:
#             consumer.close()
#             logging.error('%r generated an exception: %s' % (worker_id, exc))
#             logging.warning("Consumer %s closed" % (worker_id))
#             break
#
#     if running:
#         start_worker(worker_id)

# if __name__ == '__main__':
#     start_worker(1)
