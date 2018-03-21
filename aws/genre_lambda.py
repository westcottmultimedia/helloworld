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

    # For sales charts
    def get_chart_sales_collection_ids(self, service_id, territory_id, kind_db_table, collection_db_table, date_str): # could add in (... date_str = TODAY)
        query = """
            SELECT media_id FROM {}
            WHERE
                service_id = %s
                AND
                territory_id = %s
                AND
                date_str = %s
                AND
                media_type = %s
        """.format(collection_db_table)

        try:
            self.c.execute(query, (service_id, territory_id, date_str, kind_db_table))
            ids = []
            for row in self.c.fetchall():
                ids.append(row[0])
            return ids
        except Exception as e:
            print('failed getting collection ids:', e)
            raise
            return []

    # For streaming charts
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

    def get_playlist_collection_ids(self, playlist_id, playlist_db_table, version):
        query = """
            SELECT track_id FROM {}
            WHERE
                playlist_id = %s
                AND
                playlist_version = %s
        """.format(playlist_db_table)

        try:
            self.c.execute(query, (playlist_id, version))

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

    def get_latest_version_for_playlist(self, playlist_id):
        query = """
            SELECT latest_version FROM playlist
            WHERE
                id = %s
        """

        try:
            self.c.execute(query, [playlist_id])
            return self.c.fetchone()[0]
        except Exception as e:
            raise
            return None

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

    def territory_mapping_code_to_db_id(self, territory_code):
        query = """
            SELECT id FROM territory
            WHERE code = %s
        """

        try:
            self.c.execute(query, [territory_code])
            return self.c.fetchone()[0]
        except Exception as e:
            raise
            return None

    # ---- PLAYLISTS ----
    def get_latest_date_for_playlists(self):
        query = """
            SELECT max(date_str) FROM playlist_followers
        """
        try:
            self.c.execute(query, [])
            return self.c.fetchone()[0]
        except Exception as e:
            raise
            return None

    # "top" by follower count
    def get_top_playlist_ids(self, date_str, limit):
        query = """
            SELECT playlist_id FROM playlist_followers
            WHERE
                date_str = %s
            ORDER BY followers DESC
            LIMIT %s
        """

        try:
            self.c.execute(query, (date_str, limit))
            ids = []
            for row in self.c.fetchall():
                ids.append(row[0])
            return ids
        except Exception as e:
            print('failed getting collection ids:', e)
            raise
            return []

    def get_top_playlist_info(self, playlist_db_ids):
        query = """
            SELECT * FROM (
                SELECT DISTINCT ON (playlist.id)
                    playlist.id,
                    playlist.owner_id,
                    playlist.name,
                    po.alt_name,
                    pf.followers
                FROM playlist
                INNER JOIN playlist_owner po
                    ON po.id = playlist.owner_id
                INNER JOIN playlist_followers pf
                    ON pf.playlist_id = playlist.id
                WHERE playlist.id IN %s
            ) q
            ORDER BY followers DESC
        """

        try:
            self.c.execute(query, [tuple(playlist_db_ids)])
            infos = []
            for row in self.c.fetchall():
                info = {}
                info['playlist_db_id'] = row[0]
                info['owner_db_id'] = row[1]
                info['playlist_name'] = row[2]
                info['owner_name'] = row[3]
                info['followers'] = row[4]
                infos.append(info)

            print('get_playlist_ids info', infos)
            return infos
        except Exception as e:
            print('failed getting playlist info:', e)
            raise
            return []


class GenreRanks:
    def __init__(self, service, territory, kind, collection_type, playlist_id = None, date_str = 'latest'):
        self.db = TrackDatabase()

        # set attributes
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
        self.top_genres = []
        self.date = date_str # can also be for a specific date
        if playlist_id:
            self.playlist_id = int(playlist_id)

        # work work work
        self._map_db_tables()
        # self.inspect_attrs()

    def inspect_attrs(self):
        print('', self.playlist_id, self._kind_db_table, self._collection_type)

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
        if self._collection_type == 'streaming':
            self.collection_ids = self.db.get_chart_collection_ids(self._service, self._territory, self._kind_db_table, self._collection_db_table, latest_date)
        elif self._collection_type == 'sales':
            self.collection_ids = self.db.get_chart_sales_collection_ids(self._service, self._territory, self._kind_db_table, self._collection_db_table, latest_date)
        return True

    def load_playlist_collection_ids(self):
        latest_version = self.db.get_latest_version_for_playlist(self.playlist_id)
        self.collection_ids = self.db.get_playlist_collection_ids(self.playlist_id, self._collection_db_table, latest_version)
        return True

    def load_artist_ids(self):
        artist_ids = []
        for collection_id in self.collection_ids:
            artist_ids.append(self.db.get_artist_id_from_collection_id(self._kind_db_table, self._collection_db_table, collection_id))

        self.artist_ids = artist_ids
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
        return True

    # ranks: how many ranks to return? ie. ranks = 3 returns top 3 ranked genres
    def get_top_genres(self, ranks = 5):
        self.top_genres = sorted(self.genre_counts, reverse = True, key = self.genre_counts.__getitem__)[0:ranks]
        return self.top_genres

class Playlists:
    def __init__(self):
        self.db = TrackDatabase()
        self.top_playlists_ids = []
        self.top_playlist_infos = []
        self.latest_date_playlists = ''

        self.get_latest_date_playlists()
        self.get_playlist_ids()
        self.get_playlist_info()
        self.introspect_attrs()

    def introspect_attrs(self):
        print('introspecting', self.top_playlists_ids, self.top_playlist_infos, self.latest_date_playlists)

    def get_latest_date_playlists(self):
        self.latest_date_playlists = self.db.get_latest_date_for_playlists()

    def get_playlist_ids(self, limit = 250):
        self.top_playlists_ids = self.db.get_top_playlist_ids(self.latest_date_playlists, limit)

    def get_playlist_info(self):
        self.top_playlist_infos = self.db.get_top_playlist_info(self.top_playlists_ids)

def test_handler(event, context):
        global db

        db = TrackDatabase()
        gr_chart = GenreRanks('spotify', 'us', 'track', 'streaming')

        # get genres for charts - Spotify streaming, Apple Streaming, iTunes Sales charts
        #
        # TODO: change for sALES and STREAMING...
        gr_chart.load_chart_collection_ids()
        gr_chart.load_artist_ids()
        gr_chart.load_genres_ids()
        gr_chart.calculate_genre_counts()
        gr_chart.calculate_genre_percentage()
        gr_chart.get_top_genres()

        # get genres for playlists
        gr_playlist = GenreRanks('spotify', 'us', 'track', 'playlist', playlist_id = 2795)
        gr_playlist.load_playlist_collection_ids()
        gr_playlist.load_artist_ids()
        gr_playlist.load_genres_ids()
        gr_playlist.calculate_genre_counts()
        gr_playlist.calculate_genre_percentage()
        gr_playlist.get_top_genres()

        db.close_database()
        print('closed database connection')

def service_mapping(service_name):
    if service_name == 'spotify':
        return 1
    elif service_name == 'apple':
        return 2

def territory_mapping(territory_code):
    return db.territory_mapping_code_to_db_id(territory_code)

def genre_api_charts(service, territory_code, kind, collection_type):
    global db
    db = TrackDatabase()
    gr_chart = GenreRanks(service_mapping(service), territory_mapping(territory_code), kind, collection_type)

    # get genres for charts - Spotify streaming, Apple Streaming, iTunes Sales charts
    #
    gr_chart.load_chart_collection_ids()
    gr_chart.load_artist_ids()
    gr_chart.load_genres_ids()
    gr_chart.calculate_genre_counts()
    gr_chart.calculate_genre_percentage()

    db.close_database()
    # new format
    return {
        'genres': gr_chart.get_top_genres(),
        'genre_percentages': gr_chart.genre_percentages
    }

def genre_api_playlists(playlist_id = 2795):
    global db
    db = TrackDatabase()

    # get genres for playlists
    gr_playlist = GenreRanks(service_mapping('spotify'), 'us', 'track', 'playlist', playlist_id)
    gr_playlist.load_playlist_collection_ids()
    gr_playlist.load_artist_ids()
    gr_playlist.load_genres_ids()
    gr_playlist.calculate_genre_counts()
    gr_playlist.calculate_genre_percentage()

    db.close_database()
    return {
        'genres': gr_playlist.get_top_genres(),
        'genre_percentages': gr_playlist.genre_percentages
    }

def fetch_top_playlists(limit = 50):
    global db
    db = TrackDatabase()

    playlists = Playlists()

    db.close_database()
    return {
        'playlists': playlists.top_playlist_infos
    }

# ---- AWS LAMBDA, API GATEWAY ----
# How to structure json response
# https://goonan.io/a-simple-python3-lambda-function-on-aws-with-an-api-gateway/
def api_response(message, status_code):
    return {
        "statusCode": str(status_code),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        "body": json.dumps(message)
     }

# ----- AWS SERVERLESS HANDLERS -----
#
# serverless:
# path: /genre/test/{collection_type}/{kind}
#
# Options for path parameters:
# {collection_type} = streaming,sales
# {kind} = track, album, music video
# {service} = spotify, apple
# {territory} = two letter character code... 'na', 'gb', etc... # maybe map ids to the database ids for territory
# based on: https://v9smm139ul.execute-api.us-west-2.amazonaws.com/dev/genre/test/stream/track/
def genre_api_handler(event, context):
    try:
        collection_type = event['pathParameters']['collection_type']
        kind = event['pathParameters']['kind']
        # https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
        return api_response({
            'collection_type': collection_type,
            'kind': kind
        }, 200)
    except Exception as e:
        return api_response({'message': e.message}, 400)

# path: /genre/chart/{collection_type}/{kind}/{service}/{territory}
#
def genre_api_charts_handler(event, context):
    try:
        service = event['pathParameters']['service']
        territory = event['pathParameters']['territory']
        kind = event['pathParameters']['kind']
        collection_type = event['pathParameters']['collection_type']

        return api_response(
            genre_api_charts(service, territory, kind, collection_type),
            200
        )
    except Exception as e:
        print(e)
        return api_response({
            'genres': [],
            'genre_percentages': {}
        }, 400)

# path: /genre/playlist/:id
def genre_api_playlists_handler(event, context):
    try:
        playlist_id = event['pathParameters']['playlist_id']
        return api_response(
            genre_api_playlists(playlist_id),
            200
        )
    except Exception as e:
        print(e)
        return api_response({
            'message': e,
            'genres': [],
            'genre_percentages': {}
        }, 400)

def fetch_top_playlists_handler(event, context):
    try:
        params = event['pathParameters']
        return api_response(
            fetch_top_playlists(),
            200
        )
    except Exception as e:
        print(e)
        return api_response({
            'playlists': []
        }, 400)


# ---- LOCAL TESTING -----
if __name__ == '__main__':
    fetch_top_playlists()
