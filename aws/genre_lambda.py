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

    def get_chart_collection_ids(self, service_id, territory_id, kind_db_table, collection_db_table, date_str): # could add in (... date_str = TODAY)
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
            raise
            return []

    def get_playlist_collection_ids(self, playlist_id, playlist_db_table, date_str):
        query = """
            SELECT track_id FROM {}
            WHERE
                playlist_id = %s
                AND
                date_str = %s
        """.format(playlist_db_table)

        try:
            self.c.execute(query, (playlist_id, date_str))

            ids = []
            for row in self.c.fetchall():
                ids.append(row[0])
            return ids

        except Exception as e:
            print('failed getting playlist collection ids:', e)
            raise
            return []

    def get_latest_date_for_chart_collection(self, service_id, territory_id, kind_db_table, collection_db_table):
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
        return

    def get_latest_date_for_playlist_collection(self, playlist_id, playlist_db_table):
        query = """
            SELECT max(date_str) from {}
            WHERE
                playlist_id = %s
        """.format(playlist_db_table)

        self.c.execute(query, [playlist_id])

    def get_artist_id_from_collection_id(self, kind_db_table, collection_db_table, collection_id):
        # kind = track
        # collection is
        query = """
            SELECT artist.id FROM artist
            INNER JOIN {0} ON artist.id = {0}.artist_id
            WHERE {0}.id = %s
        """.format(kind_db_table)

        try:
            self.c.execute(query, [collection_id])
            return self.c.fetchone()[0]
        except Exception as e:
            raise
            return None

    def get_genres_from_artist_id(self, artist_id):
        query = """
            SELECT genre FROM artist_genre
            WHERE artist_id = %s
        """

        try:
            self.c.execute(query, [artist_id])

            genres = []
            for row in self.c.fetchall():
                genres.append(row[0])
                return genres

        except Exception as e:
            print('genre from artist_id error', e)
            raise

        return []

class GenreRanks:
    def __init__(self, service, territory, kind, collection_type, playlist_id = None, date_str = 'latest'):
        self.db = TrackDatabase()

        self._service = service # spotify, apple
        self._territory = territory # the territory/region code, ie. 'jp', 'dk', 'us'
        self._kind = kind # track, album, music video
        self._kind_db_table = '' # track, music video, album - psql db name of the kind object
        self._collection_type = collection_type # stream, sales VS. playlist
        self._collection_db_table = ''
        self.collection_ids = [] # db ids of collection items
        self.artist_ids = [] # list of ARTISTS in a collection of kinds (ie. stream list of songs, playlist of songs, sale list of music videos)
        self.genres = [] # list of all genres for self.artists
        self.genre_counts = {} # {genre: count, 'pop-rock': 5, ...} - counts of all genres for the list of artists
        self.genre_percentages = {}
        self.date = date_str # can also be for a specific date
        self.playlist_id = playlist_id
        self._map_db_tables()

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
    def load_chart_collection_ids(self):
        latest_date = self.db.get_latest_date_for_chart_collection(self._service, self._territory, self._kind_db_table, self._collection_db_table)
        self.collection_ids = self.db.get_chart_collection_ids(self._service, self._territory, self._kind_db_table, self._collection_db_table, latest_date)
        return True

    def load_playlist_collection_ids(self):
        latest_date = self.db.get_latest_date_for_playlist_collection(self.playlist_id, self._collection_db_table)
        self.collection_ids = self.db.get_playlist_collection_ids(self.playlist_id, self._collection_db_table, latest_date)
        return True

    def load_artist_ids(self):
        artist_ids = []
        for collection_id in self.collection_ids:
            artist_ids.append(self.db.get_artist_id_from_collection_id(self._kind_db_table, self._collection_db_table, collection_id))

        self.artist_ids = artist_ids
        print(self.artist_ids)
        return True

    def load_genres_ids(self):
        genres = []
        for artist_id in self.artist_ids:
            genres.append(self.db.get_genres_from_artist_id(artist_id))

        self.genres = genres
        return True

    def calculate_genre_counts(self):
        genre_counts = {}
        for group_of_genres in self.genres:
            for genre in group_of_genres:
                genre_counts[genre] = genre_counts.setdefault(genre, 0) + 1

        self.genre_counts = genre_counts
        return True

    def calculate_genre_percentage(self):
        total = 0
        for genre, count in self.genre_counts.items():
            total += count

        genre_percentages = {}
        for genre, count in self.genre_counts.items():
            genre_percentages[genre] = count/total

        self.genre_percentages = genre_percentages
        print(self.genre_percentages)
        return True

    # ranks: how many ranks to return? ie. ranks = 3 returns top 3 ranked genres
    def get_top_genres(self, ranks = 3):
        return sorted(self.genre_counts, reverse = True, key = self.genre_counts.__getitem__)[0:ranks]

def test_handler(event, context):
        global db

        db = TrackDatabase()
        gr_chart = GenreRanks(1, 1, 'track', 'streaming')

        # get genres for charts - Spotify streaming, Apple Streaming, iTunes Sales charts
        #
        gr_chart.load_chart_collection_ids()
        gr_chart.load_artist_ids()
        gr_chart.load_genres_ids()
        gr_chart.calculate_genre_counts()
        gr_chart.calculate_genre_percentage()
        gr_chart.get_top_genres()

        # get genres for playlists
        gr_playlist = GenreRanks(1, 1, 'track', 'playlist', playlist_id = 2795)
        gr_playlist.load_playlist_collection_ids()
        gr_playlist.load_artist_ids()
        gr_playlist.load_genres_ids()
        gr_playlist.calculate_genre_counts()
        gr_playlist.calculate_genre_percentage()
        gr_playlist.get_top_genres()

        db.close_database()
        print('closed database connection')

# ---- AWS LAMBDA, API GATEWAY ----
# How to structure json response
# https://goonan.io/a-simple-python3-lambda-function-on-aws-with-an-api-gateway/
def genre_api_response(message, status_code):
    return {
        "statusCode": str(status_code),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        "body": json.dumps(message)
     }

def genre_api_handler(event, context):
    try:
        return genre_api_response({ "message": "Hello World!" }, 200)
    except Exception as e:
        return genre_api_response({'message': e.message}, 400)
