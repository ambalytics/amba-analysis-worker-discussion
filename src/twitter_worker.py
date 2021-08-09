import logging
import math
import re
from datetime import date, datetime
from functools import lru_cache
from multiprocessing.context import Process
from difflib import SequenceMatcher

import requests
import spacy
from spacytextblob.spacytextblob import SpacyTextBlob

from collections import Counter

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


# todo heartbeat kafka?
# WARNING:kafka.coordinator:Heartbeat failed for group twitter-worker because it is rebalancing
# WARNING:kafka.coordinator:Heartbeat failed ([Error 27] RebalanceInProgressError); retrying


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


# todo check encoding, use country code
@lru_cache(maxsize=100)
def geoencode(location):
    base_url = 'https://nominatim.openstreetmap.org/search?&format=jsonv2&addressdetails=1&q='
    r = requests.get(base_url+location)
    if r.status_code == 200:
        json_response = r.json()
        if json_response and isinstance(json_response, list) and len(json_response) > 0:
            json_object = json_response[0]
            # logging.warning(json_object)
            # logging.warning('c ' + json_object['address']['country_code'])
            if 'address' in json_object and 'country_code' in json_object['address']:
                return json_object['address']['country_code']
    return None

class TwitterWorker(EventStreamConsumer, EventStreamProducer):
    state = "linked"
    relation_type = "discusses"
    publication_client = False
    log = "TwitterWorker "
    group_id = "twitter-worker"
    nlp = False

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
        # e.data['subj']['processed']['contains_abstract'] = compare_text(e.data['subj']['data']['text'],
        #                                                                 e.data['obj']['data']['abstract'])
        # # isBot
        # if e.data['subj']['data']['source'] in self.source_score:
        #     e.data['subj']['processed']['bot_rating'] = self.source_score[e.data['subj']['data']['source']]
        # else:
        #     e.data['subj']['processed']['bot_rating'] = 1

        # typeOfTweet (quote, retweet, tweet)
        if not e.data['subj']['data']['referenced_tweets'][0]['type']:
            e.data['subj']['processed']['tweet_type'] = 'tweet'
        else:
            e.data['subj']['processed']['tweet_type'] = e.data['subj']['data']['referenced_tweets'][0]['type']


        # author processing
        author_data = TwitterWorker.get_author_data(e.data['subj']['data'])
        # should be always true?
        if author_data:
            if 'location' in author_data:
                temp_location = geoencode(author_data['location'])
                if temp_location:
                    e.data['subj']['processed']['location'] = temp_location
                else:
                    e.data['subj']['processed']['location'] = 'unknown'

            e.data['subj']['processed']['followers'] = author_data['public_metrics']['followers_count']

            e.data['subj']['processed']['verified'] = 1 if author_data['verified'] else 0.7
            e.data['subj']['processed']['name'] = author_data['username']

            if 'bot' in author_data['username']:
                e.data['subj']['processed']['bot_rating'] = 1
            else:
                e.data['subj']['processed']['bot_rating'] = 0.1


        # use nltk and check for lang = eng, stop words etc
        # normalized tokenized top words from text, remove hashtags and stuff ?
        text = e.data['subj']['data']['text'].strip().lower()

        spacy_result = self.spacy_process(text, e.data['obj']['data']['abstract'])
        e.data['subj']['processed']['words'] = spacy_result['common_words']
        e.data['subj']['processed']['contains_abstract'] = spacy_result['abstract']
        e.data['subj']['processed']['sentiment'] = spacy_result['sentiment']

        # - Sentiment https://realpython.com/sentiment-analysis-python/
        # - score (how high we rank this tweet)
        # - match tweet author and publication author # compare just with text compare

        user_score = e.data['subj']['processed']['bot_rating'] * math.log(e.data['subj']['processed']['followers'], 2) * e.data['subj']['processed']['verified']
        type_score = score_type(e.data['subj']['processed']['tweet_type'])
        dt = datetime(int(split_date[0]), int(split_date[1]), int(split_date[2]))
        time_score = score_time((datetime.today() - dt).days)

        abstract_score = (2 / (e.data['subj']['processed']['contains_abstract'] + 1) - 1) * 10  # todo check or use threshold
        # abstract_score = 1 if e.data['subj']['processed']['contains_abstract'] > 0.7 else 0

        logging.warning('score %s - %s - %s - %s' % (time_score, type_score, user_score, abstract_score))

        weights = {'time': 1, 'type': 1, 'user': 1, 'abstract': 1}
        e.data['subj']['processed']['score'] = weights['time'] * time_score + weights['type'] * type_score + weights[
            'user'] * user_score + weights['abstract'] * abstract_score

        e.set('state', 'processed')
        self.publish(e)

    # https://towardsdatascience.com/text-normalization-with-spacy-and-nltk-1302ff430119
    # todo remove hashtags? remove @somebody
    def spacy_process(self, text, abstract):
        if not self.nlp:
            self.nlp = spacy.load('en_core_web_md')
            self.nlp.add_pipe('spacytextblob')

        abstract_doc = self.nlp(abstract)

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

        result = {
            'sentiment': doc._.polarity,
            'abstract': doc.similarity(abstract_doc),
            'common_words': word_freq.most_common(10)
        }

        return result

    @staticmethod
    def get_author_data(tweet_data):
        author_id = tweet_data['author_id']

        for user in tweet_data['includes']['users']:
            if user['id'] == author_id:
                return user
        return None

