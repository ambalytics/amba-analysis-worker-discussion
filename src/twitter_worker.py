import logging
import math
import numbers
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


# score higher better

def normalize(string):
    """normalize a string

    Arguments:
        string: the string to be normalized
    """
    return (re.sub('[^a-zA-Z ]+', '', string)).casefold().strip()


def score_time(x):
    """calculate a score based on a given time in days

    Arguments:
        x: the time to base the score on
    """
    if x and type(x) == int or type(x) == float:
        y = (math.log(x) / math.log(1 / 7) + 3) * 10
        if y > 30:
            return 30
        if y < 1:
            return 1
        return y
    else:
        logging.warning('missing x')
    return 1


def score_type(type):
    """calculate a score based on a given type

    Arguments:
        type: the type to base the score on
    """
    if type == 'quote':
        return 7
    if type == 'retweet':
        return 2
    # original tweet
    return 10


def score_length(length):
    """calculate a score based on a given length

    Arguments:
        length: the length to base the score on
    """
    if length < 50:
        return 3
    if length < 100:
        return 6
    return 10


@lru_cache(maxsize=100)
def geoencode(location):
    """geoencode a location

    Arguments:
        location: the location we want a country for
    """
    base_url = 'https://nominatim.openstreetmap.org/search?&format=jsonv2&addressdetails=1&q='
    r = requests.get(base_url + location)
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
    """process tweets
    """
    state = "linked"
    relation_type = "discusses"
    publication_client = False
    log = "TwitterWorker "
    group_id = "twitter-worker"
    nlp = {
        'de': None,
        'es': None,
        'en': None
    }
    process_number = 2

    def on_message(self, json_msg):
        """process a tweet

        Arguments:
            json_msg: the json_msg containing the event to be processed
        """
        logging.warning(self.log + "on message twitter consumer")

        e = Event()
        e.from_json(json_msg)

        # todo check that source_id is twitter

        e.data['subj']['processed'] = {}
        e.data['subj']['processed']['question_mark_count'] = e.data['subj']['data']['text'].count("?")
        e.data['subj']['processed']['exclamation_mark_count'] = e.data['subj']['data']['text'].count("!")
        e.data['subj']['processed']['length'] = len(e.data['subj']['data']['text'])

        split_date = e.data['obj']['data']['pubDate'].split('-')
        pub_timestamp = 2021
        if len(split_date) > 2:
            pub_timestamp = date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
        if len(split_date) == 1:
            pub_timestamp = date(int(split_date[0]), 1, 1)
            split_date = [split_date[0], 1, 1]
        else:
            # todo fix
            pub_timestamp = date(pub_timestamp, 1, 1)

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

            e.data['subj']['processed']['verified'] = 10 if author_data['verified'] else 7
            e.data['subj']['processed']['name'] = author_data['username']

            if 'bot' in author_data['username']:
                e.data['subj']['processed']['bot_rating'] = 1
            else:
                e.data['subj']['processed']['bot_rating'] = 10

        content_score = 1
        text = e.data['subj']['data']['text'].strip().lower()
        if text and e.data['obj']['data']['abstract'] and e.data['subj']['data']['lang']:
            spacy_result = self.spacy_process(text, e.data['obj']['data']['abstract'], e.data['subj']['data']['lang'])
            e.data['subj']['processed']['words'] = spacy_result['common_words']
            e.data['subj']['processed']['contains_abstract'] = self.normalize_abstract_value(spacy_result['abstract'])
            e.data['subj']['processed']['sentiment'] = self.normalize_sentiment_value(spacy_result['sentiment'])
            content_score = e.data['subj']['processed']['contains_abstract'] + e.data['subj']['processed']['sentiment']
        content_score += score_length(e.data['subj']['processed']['length'])

        user_score = e.data['subj']['processed']['bot_rating']
        if e.data['subj']['processed']['followers'] and type(e.data['subj']['processed']['followers']) == int \
                or type(e.data['subj']['processed']['followers']) == float:
            user_score += math.log(e.data['subj']['processed']['followers'], 2)
        user_score += e.data['subj']['processed']['verified']

        type_score = score_type(e.data['subj']['processed']['tweet_type'])

        dt = datetime(int(split_date[0]), int(split_date[1]), int(split_date[2]))
        time_score = score_time((datetime.today() - dt).days)

        logging.debug('score %s - %s - %s - %s' % (time_score, type_score, user_score, content_score))

        e.data['subj']['processed']['time_score'] = time_score
        e.data['subj']['processed']['type_score'] = type_score
        e.data['subj']['processed']['user_score'] = user_score
        e.data['subj']['processed']['content_score'] = content_score

        weights = {'time': 1, 'type': 1, 'user': 1, 'content': 1}
        e.data['subj']['processed']['score'] = weights['time'] * time_score
        e.data['subj']['processed']['score'] += weights['type'] * type_score
        e.data['subj']['processed']['score'] += weights['user'] * user_score
        e.data['subj']['processed']['score'] += weights['content'] * content_score

        e.set('state', 'processed')
        self.publish(e)

    @staticmethod
    def normalize_abstract_value(value):
        """normalize the calculated value from an abstract comparison

        Arguments:
            value: the value to be normalized
        """

        if value < 0:
            return 10
        if value < 0.7:
            return 5
        return 1

    @staticmethod
    def normalize_sentiment_value(value):
        """normalize the calculated value from an sentiment comparison

        Arguments:
            value: the value to be normalized
        """
        if value > 0.5:
            return 10
        if value < -0.5:
            return 1
        return 5

    # https://towardsdatascience.com/text-normalization-with-spacy-and-nltk-1302ff430119
    # https://towardsdatascience.com/twitter-sentiment-analysis-a-tale-of-stream-processing-8fd92e19a6e6
    # todo switch depending on languages
    # remove rt, min word letter count 3?, remove links
    def spacy_process(self, text, abstract, lang):
        """process a tweet using spacy

        Arguments:
            text: the tweet text
            abstract: the publication abstract
            lang: language of the tweet
        """
        local_nlp = None
        if lang is 'de':
            if not self.nlp[lang]:
                self.nlp[lang] = spacy.load('de_core_news_md')
                self.nlp[lang].add_pipe('spacytextblob')
            local_nlp = self.nlp[lang]
        elif lang is 'es':
            if not self.nlp[lang]:
                self.nlp[lang] = spacy.load('es_core_news_md')
                self.nlp[lang].add_pipe('spacytextblob')
            local_nlp = self.nlp[lang]
        else:
            if not self.nlp['en']:
                self.nlp['en'] = spacy.load('en_core_web_md')
                self.nlp['en'].add_pipe('spacytextblob')
            local_nlp = self.nlp['en']

        # todo use language from the publication not the tweet to compare?
        # does language for comparison matter?
        abstract_doc = local_nlp(abstract)
        doc = local_nlp(text)

        # Tokenization and lemmatization are done with the spacy nlp pipeline commands
        lemma_list = []
        for token in doc:
            lemma_list.append(token.lemma_)
        # print("Tokenize+Lemmatize:")
        # print(lemma_list)

        # Filter the stopword
        filtered_sentence = []
        for word in lemma_list:
            lexeme = local_nlp.vocab[word]
            if lexeme.is_stop == False:
                filtered_sentence.append(word)

        # Remove punctuation, remove links, remove words with 1 or 2 letters
        punctuations = "?:!.,;"
        for word in filtered_sentence:
            # logging.warning('wordlen %s - %s ' % (word, str(len(word))))
            if word in punctuations or 'http' in word or len(word) < 3:
                # logging.warning('remove %s', word)
                filtered_sentence.remove(word)


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
        """get the author data of a tweet

        Arguments:
            tweet_data: the tweet data we want an author from
        """
        author_id = tweet_data['author_id']

        for user in tweet_data['includes']['users']:
            if user['id'] == author_id:
                return user
        return None
