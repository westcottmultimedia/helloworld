import sys
sys.path.insert(0, './aws_packages') # local relative path of aws lambda packages for zipping

import csv, codecs, re, json, os, base64, time, hashlib, ssl, datetime, logging, errno, random
from pprint import pprint as pprint
from datetime import datetime, date, timedelta
import psycopg2

class TrackDatabase(object):

    def __init__(self):
        super(TrackDatabase, self).__init__()
        self.init_database()

    def init_database(self):
        rds_host  = "beats.cekfuk4kqawy.us-west-2.rds.amazonaws.com"
        name = "beatsdj"
        password = "beatsdj123"
        db_name = "beats"

        try:
            print('Connecting to the RDS PostgreSQL database {}...'.format(rds_host))
            self.db = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name)
            print('Successfully connected to AWS RDS PostgreSQL instance.', rds_host)
            self.db.autocommit = True
            self.c = self.db.cursor()
        except Exception as e:
            print(e)

    def close_database(self):
        try:
            if self.db:
                self.db.close()
        except:
            print('cannot close db')
        return True

    def get_collection_ids(self, service_id, territory_id, kind_db_table, collection_db_table, date_str): # could add in (... date_str = TODAY)
        query = """
            SELECT {}_id FROM {}
            WHERE
                service_id = %s
                AND
                territory_id = %s
                AND
                date_str = %s
        """.format(kind_db_table, collection_db_table)

        try:
            self.c.execute(query, (service_id, territory_id, date_str))
            ids = []

            for row in self.c.fetchall():
                ids.append(row[0])
            return ids
        except Exception as e:
            print('failed getting collection ids:', e)
            return []

    def get_latest_date_for_collection(self, service_id, territory_id, kind_db_table, collection_db_table):
        query = """
            SELECT max(date_str) FROM {}
            WHERE
                service_id = %s
                AND
                territory_id = %s
        """.format(collection_db_table)

        self.c.execute(query, (service_id, territory_id))

        row = self.c.fetchone()
        if row:
            return row[0]

    def get_artist_ids_for_collection_ids():
        return

class GenreRanks:
    def __init__(self, service, territory, kind, collection_type, date_str = 'latest'):
        self._service = service # spotify, apple
        self._territory = territory # the territory/region code, ie. 'jp', 'dk', 'us'
        self._kind = kind # track, album, music video
        self._kind_db_table = '' # track, music video, album - psql db name of the kind object
        self._collection_type = collection_type # stream, sales VS. playlist
        self._collection_db_table = ''
        self.collection_ids = [] # db ids of collection items
        self.artists = [] # list of ARTISTS in a collection of kinds (ie. stream list of songs, playlist of songs, sale list of music videos)
        self.genres = [] # list of all genres for self.artists
        self.genre_counts = {} # {genre: count, 'pop-rock': 5, ...} - counts of all genres for the list of artists
        self.date = date_str # can also be for a specific date
        self.db = TrackDatabase()

        self._map_db_tables()
        self.parrot_attributes()

    def parrot_attributes(self):
        print('parroting...', self._kind_db_table, self._collection_db_table)

    def _map_db_tables(self):
        if self._collection_type == 'streaming':
            self._kind_db_table = 'track'
            self._collection_db_table = 'track_position'

        elif self._collection_type == 'sales':
            self._collection_db_table = 'sales_position'

            if self._kind == 'track':
                self._kind_db_table = 'track'
            elif self._kind == 'album':
                self._kind_db_table = 'album'
            elif self._kind == 'music_video':
                self._kind_db_table = 'music_video'

        elif self._collection_type == 'playlist':
            self._kind_db_table = 'track'
            self._collection_db_table = 'playlist_track_position'
        else:
            print('incorrect kind: {} or collection type: {})'.format(self._kind, self._collection_type))
            raise

        # populate kind db table and collection_db_table

    # For everything NOT a playlist
    def load_collection_ids(self):
        latest_date = self.db.get_latest_date_for_collection(self._service, self._territory, self._kind_db_table, self._collection_db_table)
        self.collection_ids = self.db.get_collection_ids(self._service, self._territory, self._kind_db_table, self._collection_db_table, latest_date)
        return True

def test_handler(event, context):
        global db

        db = TrackDatabase()
        gr = GenreRanks(1, 1, 'track', 'streaming')

        gr.parrot_attributes()
        gr.load_collection_ids()

        db.close_database()
        print('closed database connection')
