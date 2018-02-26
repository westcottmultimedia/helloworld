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

class GenreRanks:
    def __init__(self, service, kind, collection_type, kind_db_table, collection_db_table):
        self._service = service # spotify, apple
        self._kind = kind # song, album, music video
        self._collection_type = collection_type # stream, playlist, sales
        self._kind_db_table = kind_db_table # track, music video, album - psql db name of the kind object
        self._collection_db_table = collection_db_table
        self.artists = [] # list of ARTISTS in a collection of kinds (ie. stream list of songs, playlist of songs, sale list of music videos)
        self.genres = [] # list of all genres for self.artists
        self.genre_counts = {} # {genre: count, 'pop-rock': 5, ...} - counts of all genres for the list of artists


    def parrot_attributes(self):
        print('parroting...', self._service, self._kind, self._collection_type)

def test_handler(event, context):
        global db

        db = TrackDatabase()
        gr = GenreRanks('spotify', 'song', 'streaming', 'track', 'track_position')

        gr.parrot_attributes()

        db.close_database()
        print('closed database connection')
