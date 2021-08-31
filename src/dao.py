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

        Session = sessionmaker(bind=engine)
        self.session = Session()

    def save_object(self, obj):
        try:
            self.session.add(obj)
            return self.session.commit()
        except IntegrityError:
            self.session.rollback()
        return None

    def get_object(self, table, key):
        result = self.session.query(table).filter_by(**key).first()
        if not result:
            return None
        return result

    def save_if_not_exist(self, obj, table, kwargs):
        obj_db = self.get_object(table, kwargs)
        if obj_db:
            print('does exists')
            return obj_db
        else:
            print('does not exists')
            return self.save_object(obj)

    def save_discussion_data(self, event_data):
        # use doi or id?
        event_data['sourceId'] = 'twitter'  # todo
        if 'location' not in event_data['subj']['processed']:
            event_data['subj']['processed']['location'] = 'unknown'
        if 'contains_abstract' not in event_data['subj']['processed']:
            event_data['subj']['processed']['contains_abstract'] = 0
        if 'name' not in event_data['subj']['processed']:
            event_data['subj']['processed']['name'] = 'unknown'

        discussion_data = DiscussionData(
            id=event_data['id'],
            publicationDoi=event_data['obj']['data']['doi'],
            createdAt=event_data['timestamp'],
            score=event_data['subj']['processed']['score'],
            # time_score
            # type_score
            # user_score
            abstractDifference=event_data['subj']['processed']['contains_abstract'],
            length=event_data['subj']['processed']['length'],
            questions=event_data['subj']['processed']['question_mark_count'],
            exclamations=event_data['subj']['processed']['exclamation_mark_count'],
            type=event_data['subj']['processed']['tweet_type'],
            sentiment=event_data['subj']['processed']['sentiment'],
            subjId=event_data['subj']['alternative-id'],
            followers=event_data['subj']['processed']['followers'],
            verified=event_data['subj']['processed']['verified'],
            botScore=event_data['subj']['processed']['bot_rating'],
            authorName=event_data['subj']['processed']['name'],  # make this a table?
            authorLocation=event_data['subj']['processed']['location'],
            sourceId=event_data['sourceId'])
        discussion_data = self.save_if_not_exist(discussion_data, Publication, {'id': discussion_data.id})

        context_entity = event_data['context_entity']
        for entity_data in context_entity:
            entity = DiscussionEntity(name=entity_data['subj']['data']['context_annotations']['entity']['name'])

            entity = self.save_if_not_exist(entity, DiscussionEntity, {'name': entity.name})

            publication_entity = DiscussionEntityData(
                **{'discussionDataId': discussion_data.id, 'discussionEntityId': entity.id})
            self.save_object(publication_entity)

        words = event_data['words']
        for words_data in words:
            word = DiscussionWord(word=words_data[0])
            word = self.save_if_not_exist(word, DiscussionWord, {'word': word.word})
            publication_words = DiscussionWordData(
                **{'discussionDataId': discussion_data.id, 'discussionWordId': word.id, 'count': words_data[1]})
            self.save_object(publication_words)

        hashtags = event_data['hashtags']
        for h_data in hashtags:
            hashtag = DiscussionHashtag(hashtag=h_data['subj']['data']['entities']['hashtags']['tag'])
            hashtag = self.save_if_not_exist(hashtag, DiscussionHashtag, {'hashtag': hashtag.hashtag})
            publication_h = DiscussionHashtagData(
                **{'discussionDataId': discussion_data.id, 'iscussionHashtagId': hashtag.doi})
            self.save_object(publication_h)

        return discussion_data
