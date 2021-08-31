from event_stream.models.model import *
from sqlalchemy import Table, Column, MetaData, create_engine
import os
import urllib
import psycopg2
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker


class DAO(object):

    def __init__(self):
        host_server = os.environ.get('POSTGRES_HOST', 'postgres')
        db_server_port = urllib.parse.quote_plus(str(os.environ.get('POSTGRES_PORT', '5432')))
        database_name = os.environ.get('POSTGRES_DB', 'amba')
        db_username = urllib.parse.quote_plus(str(os.environ.get('POSTGRES_USER', 'streams')))
        db_password = urllib.parse.quote_plus(str(os.environ.get('POSTGRES_PASSWORD', 'REPLACE_ME')))

        # ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
        DATABASE_URL = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(db_username, db_password, host_server,
                                                                     db_server_port, database_name)
        print(DATABASE_URL)

        # engine = create_engine('postgresql+psycopg2://streams:REPLACE_ME@postgres:5432/amba')
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(engine)
        # database = databases.Database(DATABASE_URL)

        Session = sessionmaker(bind=engine, autoflush=True)
        self.session = Session()

    def save_object(self, obj):
        # try:
        self.session.add(obj)
        self.session.commit()
        # except IntegrityError:
        #     print('error')
        #     self.session.rollback()


    def get_object(self, table, key):
        result = self.session.query(table).filter_by(**key).first()
        if not result:
            return None
        return result

    def save_if_not_exist(self, obj, table, kwargs):
        obj_db = self.get_object(table, kwargs)
        if obj_db:
            # print('does exists')
            return obj_db
        else:
            # print('does not exists')
            self.save_object(obj)
            return obj


    def save_discussion_data(self, event_data):
        # use doi or id?
        event_data['sourceId'] = 'twitter'  # todo
        if 'location' not in event_data['subj']['processed']:
            event_data['subj']['processed']['location'] = 'unknown'
        if 'contains_abstract' not in event_data['subj']['processed']:
            event_data['subj']['processed']['contains_abstract'] = 0
        if 'sentiment' not in event_data['subj']['processed']:
            event_data['subj']['processed']['sentiment'] = 0
        if 'name' not in event_data['subj']['processed']:
            event_data['subj']['processed']['name'] = 'unknown'

        discussion_data = DiscussionData(
            publicationDoi=event_data['obj']['data']['doi'],
            createdAt=event_data['timestamp'],
            score=event_data['subj']['processed']['score'],
            timeScore=event_data['subj']['processed']['time_score'],
            typeScore=event_data['subj']['processed']['type_score'],
            userScore=event_data['subj']['processed']['user_score'],
            abstractDifference=event_data['subj']['processed']['contains_abstract'],
            length=event_data['subj']['processed']['length'],
            questions=event_data['subj']['processed']['question_mark_count'],
            exclamations=event_data['subj']['processed']['exclamation_mark_count'],
            type=event_data['subj']['processed']['tweet_type'],
            sentiment=event_data['subj']['processed']['sentiment'],
            subjId=event_data['subj']['alternative-id'],
            followers=event_data['subj']['processed']['followers'],
            botScore=event_data['subj']['processed']['bot_rating'],
            authorName=event_data['subj']['processed']['name'],  # make this a table?
            authorLocation=event_data['subj']['processed']['location'],
            sourceId=event_data['sourceId'])
        self.save_object(discussion_data)
        print('discussion_data.id')
        print(discussion_data.id)

        if 'context_annotations' in event_data['subj']['data']:
            context_entity = event_data['subj']['data']['context_annotations']
            for entity_data in context_entity:
                entity = DiscussionEntity(entity=entity_data['entity']['name'])

                entity = self.save_if_not_exist(entity, DiscussionEntity, {'entity': entity.entity})
                print('entity.id')
                print(entity.id)

                publication_entity = DiscussionEntityData(
                    **{'discussionDataId': discussion_data.id, 'discussionEntityId': entity.id})
                self.save_object(publication_entity)

        if 'words' in event_data['subj']['processed']:
            words = event_data['subj']['processed']['words']
            for words_data in words:
                word = DiscussionWord(word=words_data[0])
                word = self.save_if_not_exist(word, DiscussionWord, {'word': word.word})
                print('word.id')
                print(word.id)
                publication_words = DiscussionWordData(
                    **{'discussionDataId': discussion_data.id, 'discussionWordId': word.id, 'count': words_data[1]})
                self.save_object(publication_words)

        if 'entities' in event_data['subj']['data'] and 'hashtags' in event_data['subj']['data']['entities']:
            hashtags = event_data['subj']['data']['entities']['hashtags']
            for h_data in hashtags:
                hashtag = DiscussionHashtag(hashtag=h_data['tag'])
                hashtag = self.save_if_not_exist(hashtag, DiscussionHashtag, {'hashtag': hashtag.hashtag})
                print('hashtag.id')
                print(hashtag.id)
                publication_h = DiscussionHashtagData(
                    **{'discussionDataId': discussion_data.id, 'discussionHashtagId': hashtag.id})
                self.save_object(publication_h)

        return discussion_data
