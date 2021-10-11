import logging
import math
import os
import sentry_sdk
import re
from datetime import date, datetime
from functools import lru_cache
import requests
import spacy
from spacytextblob.spacytextblob import SpacyTextBlob
from collections import Counter

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event

from event_stream.dao import DAO


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
        y = 0
        try:
            y = (math.log(x) / math.log(1 / 7) + 3) * 10
        except ValueError:
            logging.warning('ValueError %s' % str(x))
        if y > 30:
            return 30
        if y < 1:
            return 1
        return y
    # else:
    # logging.warning('missing x')
    return 1


def score_type(type):
    """calculate a score based on a given type

    Arguments:
        type: the type to base the score on
    """
    if type == 'quoted':
        return 7
    if type == 'replied_to':
        return 9
    if type == 'retweeted':
        return 2
    # original tweet
    # logging.warning(type)
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


@lru_cache(maxsize=10000)
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
        'de': None, 'es': None, 'en': None, 'fr': None, 'ja': None, 'it': None, 'ru': None, 'pl': None
    }
    process_number = 1

    dao = None

    def on_message(self, json_msg):
        """process a tweet
        Arguments:
            json_msg: the json_msg containing the event to be processed
        """
        #
        if not self.dao:
            self.dao = DAO()
            logging.warning(self.log + " create dao")

        # logging.warning(self.log + "on message twitter consumer")

        e = Event()
        e.from_json(json_msg)

        if e.get('source_id') == 'twitter':
            e.data['subj']['processed'] = {}
            e.data['subj']['processed']['question_mark_count'] = e.data['subj']['data']['text'].count("?")
            e.data['subj']['processed']['exclamation_mark_count'] = e.data['subj']['data']['text'].count("!")
            e.data['subj']['processed']['length'] = len(e.data['subj']['data']['text'])

            pub_timestamp = date(2012, 1, 1)
            if 'year' in e.data['obj']['data']:
                pub_timestamp = date(e.data['obj']['data']['year'], 1, 1)

            if 'pub_date' in e.data['obj']['data'] and e.data['obj']['data']['pub_date']:
                split_date = e.data['obj']['data']['pub_date'].split('-')
                if len(split_date) > 2:
                    pub_timestamp = date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
            else:
                logging.warning('publication data is missing pub_date')
                logging.warning(e.data)

            # todo use date from twitter not today
            e.data['subj']['processed']['time_past'] = (date.today() - pub_timestamp).days

            hashtags = []
            annotations = []
            a_types = []

            if 'entities' in e.data['subj']['data']:
                if 'hashtags' in e.data['subj']['data']['entities']:
                    for tag in e.data['subj']['data']['entities']['hashtags']:
                        hashtags.append(normalize(tag['tag']))

                if 'annotations' in e.data['subj']['data']['entities']:
                    for tag in e.data['subj']['data']['entities']['annotations']:
                        annotations.append(tag['normalized_text'])
                        a_types.append(tag['type'])

            e.data['subj']['processed']['hashtags'] = hashtags
            e.data['subj']['processed']['annotations'] = annotations
            e.data['subj']['processed']['a_types'] = a_types

            context_a_domain = []
            context_a_entity = []
            e.data['subj']['processed']['context_domain'] = context_a_domain
            e.data['subj']['processed']['context_entity'] = context_a_entity

            # typeOfTweet (quote, retweet, tweet)
            if 'referenced_tweets' in e.data['subj']['data'] and len(e.data['subj']['data']['referenced_tweets']) > 0 \
                    and 'type' in e.data['subj']['data']['referenced_tweets'][0]:
                e.data['subj']['processed']['tweet_type'] = e.data['subj']['data']['referenced_tweets'][0]['type']
            else:
                e.data['subj']['processed']['tweet_type'] = 'tweet'

            if e.data['subj']['data']['conversation_id'] == e.data['subj']['pid']:
                logging.warning('conversation id matches id -> tweet')

            # author processing
            author_data = TwitterWorker.get_author_data(e.data['subj']['data'])
            # should be always true?
            e.data['subj']['processed']['location'] = 'unknown'
            e.data['subj']['processed']['followers'] = 0
            e.data['subj']['processed']['bot_rating'] = 1
            if author_data:
                if 'location' in author_data:
                    temp_location = geoencode(author_data['location'])
                    if temp_location:
                        e.data['subj']['processed']['location'] = temp_location

                e.data['subj']['processed']['followers'] = author_data['public_metrics']['followers_count']

                e.data['subj']['processed']['verified'] = 10 if author_data['verified'] else 7
                e.data['subj']['processed']['name'] = author_data['username']

                if 'bot' not in author_data['username'].lower() and 'bot' not in e.data['subj']['data']['source']:
                    e.data['subj']['processed']['bot_rating'] = 10

            content_score = 1
            text = e.data['subj']['data']['text'].strip().lower()
            if text and 'abstract' in e.data['obj']['data'] and 'lang' in e.data['subj']['data']:
                spacy_result = self.spacy_process(text, e.data['obj']['data']['abstract'],
                                                  e.data['subj']['data']['lang'])
                e.data['subj']['processed']['words'] = spacy_result['common_words']
                e.data['subj']['processed']['contains_abstract_raw'] = spacy_result['abstract']
                e.data['subj']['processed']['contains_abstract'] = self.normalize_abstract_value(
                    spacy_result['abstract'])
                e.data['subj']['processed']['sentiment_raw'] = spacy_result['sentiment']
                e.data['subj']['processed']['sentiment'] = self.normalize_sentiment_value(spacy_result['sentiment'])
                content_score = e.data['subj']['processed']['contains_abstract'] + e.data['subj']['processed'][
                    'sentiment']
            content_score += score_length(e.data['subj']['processed']['length'])

            user_score = e.data['subj']['processed']['bot_rating']
            if e.data['subj']['processed']['followers'] and type(e.data['subj']['processed']['followers']) == int \
                    or type(e.data['subj']['processed']['followers']) == float:
                user_score += math.log(e.data['subj']['processed']['followers'], 2)
            user_score += e.data['subj']['processed']['verified']

            type_score = score_type(e.data['subj']['processed']['tweet_type'])

            time_score = score_time(e.data['subj']['processed']['time_past'])

            # logging.debug('score %s - %s - %s - %s' % (time_score, type_score, user_score, content_score))

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
            self.dao.save_discussion_data(e.data)
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
        if value > 0.33:
            return 10
        if value < -0.33:
            return 0
        return 5

    def spacy_process(self, text, abstract, lang):
        """process a tweet using spacy

        Arguments:
            text: the tweet text
            abstract: the publication abstract
            lang: language of the tweet
        """
        result = {
            'sentiment': 0,
            'abstract': 0,
            'common_words': []
        }
        if not text or not abstract or not lang:
            return result

        local_nlp = None
        # https://spacy.io/universe/project/spacy-langdetect
        # in case we have an undefined language

        supported = ['de', 'es', 'en', 'fr', 'ja', 'it', 'ru', 'pl']
        if 'en' not in lang and lang in supported:
            if lang not in self.nlp or not self.nlp[lang]:
                self.nlp[lang] = spacy.load(lang + '_core_news_md')
                self.nlp[lang].add_pipe('spacytextblob')
            local_nlp = self.nlp[lang]
        elif 'en' in lang:
            if not self.nlp['en']:
                self.nlp['en'] = spacy.load('en_core_web_md')
                self.nlp['en'].add_pipe('spacytextblob')
            local_nlp = self.nlp['en']
        else:
            # neutral results if we have an unknown language
            logging.debug('unknown language')
            return result

        # https://www.trinnovative.de/blog/2020-09-08-natural-language-processing-mit-spacy.html
        words = TwitterWorker.process_text_for_similarity(local_nlp, text)
        abstract_words = TwitterWorker.process_text_for_similarity(local_nlp, abstract)

        word_freq = Counter(words)

        sim = 0
        tweet_doc = local_nlp(" ".join(words))
        abstract_doc = local_nlp(" ".join(abstract_words))

        if abstract_doc:
            sim = tweet_doc.similarity(abstract_doc)

        result = {
            'sentiment': tweet_doc._.polarity,
            'abstract': sim,
            'common_words': word_freq.most_common(10)
        }

        return result

    @staticmethod
    def process_text_for_similarity(nlp, text):
        doc = nlp(text)
        if doc:
            return [token.lemma_.lower() for token in doc if not token.is_stop and not token.is_punct
                    and not token.is_space and len(token.lemma_) > 2 and not token.lemma_.lower().startswith('http')
                    and token.lemma_.lower() != 'the'
                    and (token.lemma_.isalpha() or token.lemma_.startswith('@'))
                    and (token.pos_ == "NOUN" or token.pos_ == "PROPN" or token.pos_ == "VERB")]
        return []

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

    @staticmethod
    def start(i=0):
        """start the consumer
        """
        esc = TwitterWorker(i)

        # for key, value in esc.nlp.items():
        #     if key is 'en':
        #         esc.nlp['en'] = spacy.load('en_core_web_md')
        #     else:
        #         esc.nlp[key] = spacy.load(key + '_core_news_md')
        #     esc.nlp[key].add_pipe('spacytextblob')

        logging.warning(TwitterWorker.log + 'Start %s' % str(i))
        esc.consume()


if __name__ == '__main__':
    SENTRY_DSN = os.environ.get('SENTRY_DSN')
    SENTRY_TRACE_SAMPLE_RATE = os.environ.get('SENTRY_TRACE_SAMPLE_RATE')
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=SENTRY_TRACE_SAMPLE_RATE
    )

    TwitterWorker.start(0)
