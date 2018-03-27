import sys
sys.path.insert(0, './aws_packages') # local relative path of aws lambda packages for zipping

import csv, codecs, re, json, os, base64, time, hashlib, ssl, datetime, logging, errno, random
from pprint import pprint as pprint
from datetime import datetime, date, timedelta
import psycopg2

# TODAY is day in UTC - 8hrs, or PST
TODAY = (datetime.utcnow() - timedelta(hours=8)).strftime('%Y-%m-%d')
YESTERDAY = (datetime.utcnow() - timedelta(days = 1, hours=8)).strftime('%Y-%m-%d')


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

    # CHARTS
    # ---- streaming ----
    # spotifycharts.com/regional
    # apple-music/top-songs
    # ---- sales ----
    # itunes-music/top-songs
    # music-videos/top-music-videos
    # itunes-music/top-albums
    #
    def get_processed_count(self, chart, date_str):
        query = """
            SELECT count(*) FROM processed
            WHERE
                url like '%{}%'
                AND
                url like '%{}%'
        """.format(date_str, chart)

        try:
            self.c.execute(query)
            return self.c.fetchone()[0]
        except psycopg2.InterfaceError as e:
            self.c = self.db.cursor()
            self.c.execute(query)
            return self.c.fetchone()[0]
        except Exception as e:
            print(e.message, ': failed getting {} chart count for {}'.format(chart, date_str))
            return 0

    def close_database(self):
        try:
            if self.db:
                self.db.close()
        except:
            print('cannot close db')
        return True


class ChartProcessedStatus:
    def __init__(self, service, chart_type, kind, date_str = YESTERDAY):
        self.db = TrackDatabase()

        # set attributes
        self._service = service # spotify, apple
        self._chart_type = chart_type # streaming, sales
        self._kind = kind # track, album, music_video
        self._processed_fragment = ''
        self._date = date_str
        self.processed_count = None

        self._map_processed_fragment()
        self.get_processed_count(self._processed_fragment, self._date)

        self.db.close_database()

    def _map_processed_fragment(self):
        if self._chart_type == 'streaming':
            if self._service == 'spotify':
                self._processed_fragment = 'spotifycharts.com/regional'
            elif self._service == 'apple':
                self._processed_fragment = 'apple-music/top-songs'
        elif self._chart_type == 'sales':
            if self._kind == 'track':
                self._processed_fragment = 'itunes-music/top-songs'
            elif self._kind == 'album':
                self._processed_fragment = 'itunes-music/top-albums'
            elif self._kind == 'music_video':
                self._processed_fragment = 'music-videos/top-music-videos'

    def get_processed_count(self, processed_fragment, date_str):
        self.processed_count = self.db.get_processed_count(processed_fragment, date_str)

#
def count_all_chart_processed_by_date(date_str):
    return {
        'spotify_streaming': ChartProcessedStatus('spotify', 'streaming', 'track', date_str).processed_count,
        'apple_streaming': ChartProcessedStatus('apple', 'streaming', 'track', date_str).processed_count,
        'itunes_track_sales': ChartProcessedStatus('apple', 'sales', 'track', date_str).processed_count,
        'itunes_album_sales': ChartProcessedStatus('apple', 'sales', 'album', date_str).processed_count,
        'itunes_music_video_sales': ChartProcessedStatus('apple', 'sales', 'music_video', date_str).processed_count
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

# path: {API_URL}/status
#
def chart_status_handler(event, context):
    try:
        # COLLECT EVENT PARAMETERS HERE ie.
        #
        date = YESTERDAY
        if event['pathParameters']:
            if event['pathParameters'].get('date'):
                date = event['pathParameters']['date']

        return api_response(
            count_all_chart_processed_by_date(date),
            200
        )
    except Exception as e:
        print(e)
        return api_response({
            'spotify_streaming': None,
            'apple_streaming': None,
            'itunes_track_sales': None,
            'itunes_album_sales': None,
            'itunes_music_video_sales': None
        }, 400)

# ---- LOCAL TESTING -----
if __name__ == '__main__':

    print(chart_status_handler(
        {
          "pathParameters": {
            # DEFINE EVENT PARAMETERS HERE, ie.
            #
            # "service": "apple",
          }
        }, {}))
