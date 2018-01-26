import sys
sys.path.insert(0, './aws_packages') # local relative path of aws lambda packages

import csv, codecs, re, json, os, base64, time, hashlib, ssl, jwt, configparser, logging, psycopg2, pytz
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from pprint import pprint
from datetime import datetime, date, timedelta
from dateutil import parser
from jwt.contrib.algorithms.py_ecdsa import ECAlgorithm

# logging config for writing to a log file
logging.basicConfig(level=logging.DEBUG, format='LOGGING: %(asctime)s - %(message)s')

logger = logging.getLogger()
# hdlr = logging.FileHandler('./apple/apple_api.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# hdlr.setFormatter(formatter)
# logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# cache http requests?
CACHE_ENABLED = False

# TODAY is day in UTC - 8hrs, or PST
# https://julien.danjou.info/blog/2015/python-and-timezones
TODAY = (datetime.utcnow() - timedelta(hours=8)).strftime('%Y-%m-%d')

# the Apple API url
# ie. https://api.music.apple.com/v1/catalog/{storefront}/genres/{id}
API_url = 'https://api.music.apple.com/v1/catalog/{region}/{media}'

# Apple RSS Feed url
# ie. https://rss.itunes.apple.com/api/v1/us/apple-music/hot-tracks/all/100/explicit.json
RSS_url = 'https://rss.itunes.apple.com/api/v1/{region}/{media}/{chart}/{genre}/{limit}/explicit.json'

# the regions to download
REGIONS = ["us", "gb", "vn", "mn", "za", "mz", "mr", "tw", "fm", "sg", "gw", "cn", "kg", "jp", "fj",
    "hk", "gm", "mx", "co", "mw", "ru", "ve", "kr", "la", "in", "lr", "ar", "sv", "br",
    "gt", "ec", "pe", "do", "hu", "cl", "tr", "ae", "th", "id", "pg", "my", "na", "ph",
    "pw", "sa", "ni", "py", "pk", "hn", "st", "pl", "jm", "sc", "eg", "kz", "uy", "mo",
    "ee", "lv", "kw", "hr", "il", "ua", "lk", "ro", "lt", "np", "pa", "md", "am", "mt", "cz",
    "jo", "bw", "bg", "ke", "lb", "mk", "qa", "mg", "cr", "sk", "ne", "sn", "si", "ml", "mu",
    "ai", "bs", "tn", "ug", "bb", "bm", "ag", "dm", "gd", "vg", "ky", "lc", "ms", "kn", "bn",
    "tc", "gy", "vc", "tt", "bo", "cy", "sr", "bz", "is", "bh", "it", "ye", "fr", "dz", "de",
    "ao", "ng", "om", "be", "sl", "fi", "az", "sb", "by", "at", "uz", "tm", "zw",
    "gr", "sz", "ie", "tj", "au", "td", "nz", "cg", "cv", "pt", "es", "al", "lu", "tz", "nl",
    "gh", "no", "bf", "dk", "kh", "ca", "bj", "se", "bt", "ch"]

# Regions that do not have apple music top songs, top albums charts or music videos.
REGIONS_WITHOUT_APPLE_MUSIC = ['mz', 'mr', 'mw', 'lr', 'na', 'pw', 'pk', 'st',
'jm', 'sc', 'uy', 'kw', 'hr', 'mk', 'qa', 'mg', 'sn', 'ml', 'bs', 'tn', 'lc', 'ms', 'bn', 'tc', 'gy',
'vc', 'sr', 'is', 'ye', 'dz', 'ao', 'sl', 'sb', 'td', 'cg', 'al', 'tz', 'bf', 'bj', 'bt']
REGIONS_WITHOUT_ITUNES_MUSIC = ['mr','cn','mw','kr','lr','pw','pk','st','jm','sc','uy','kw','hr','mk','mg',
'sn','ml','tn','lc','ms','tc','gy','vc','sr', 'is','ye','dz','ao','sl','sb','td','cg','al','tz','bj','bt']
REGIONS_WITHOUT_MUSIC_VIDEOS = ['mr','gw','cn','mw','kr','lr','pw','pk','st','jm','sc','uy','kw',
'hr','np','lb','mk','qa','mg','sn','ml','tn','lc','ms','tc','gy','vc','sr','is','ye','dz','ao','om',
'sl','sb','td','cg','al','tz','bj','bt']

REGIONS_WITH_APPLE_MUSIC = list(set(REGIONS).difference(REGIONS_WITHOUT_APPLE_MUSIC))
REGIONS_WITH_ITUNES_MUSIC_ALBUMS = list(set(REGIONS).difference(REGIONS_WITHOUT_ITUNES_MUSIC))
REGIONS_WITH_MUSIC_VIDEOS = list(set(REGIONS).difference(REGIONS_WITHOUT_MUSIC_VIDEOS))

CHARTS = [
    ('apple-music', 'top-songs', REGIONS_WITH_APPLE_MUSIC),
    ('itunes-music', 'top-songs', REGIONS_WITH_ITUNES_MUSIC_ALBUMS),
    ('itunes-music', 'top-albums', REGIONS_WITH_ITUNES_MUSIC_ALBUMS),
    ('music-videos', 'top-music-videos', REGIONS_WITH_MUSIC_VIDEOS)
]


# # LEGACY use of Missed Regions to manually run local custom CHARTS
#
# REGIONS_MISSED_APPLE_MUSIC = ['jo', 'kz', 'gd']
# REGIONS_MISSED_ITUNES_SONGS = []
# REGIONS_MISSED_ITUNES_ALBUMS = []
# REGIONS_MISSED_MUSIC_VIDEOS = []

# (media, chart, regions)
# CHARTS = [
#     ('apple-music', 'top-songs', REGIONS_MISSED_APPLE_MUSIC),
#     ('itunes-music', 'top-songs', REGIONS_MISSED_ITUNES_SONGS),
#     ('itunes-music', 'top-albums', REGIONS_MISSED_ITUNES_ALBUMS),
#     ('music-videos', 'top-music-videos', REGIONS_MISSED_MUSIC_VIDEOS)
# ]

# max number of times to retry http requests
MAX_url_RETRIES = 10

# seconds to wait between retry attempts
SECONDS_BETWEEN_RETRIES = 3

# APPLE CLASS START
#
#
class Apple(object):

    def __init__(self):
        super(Apple, self).__init__()
        self.authorize()

    def authorize(self):
        """
        Use credentials to get developer token
        """

        # NOTE: Since AWS Lambda does not allow the python 'cryptography' build,
        # we use the legacy EC algorithms which we register manually.

        algorithm = 'ES256'
        # if algorithm in jwt._algorithms:
        jwt.unregister_algorithm(algorithm) # needs to be unregistered first for some reason
        jwt.register_algorithm(algorithm, ECAlgorithm(ECAlgorithm.SHA256))

        # We converted our original .p8 file from Apple's API key to a EC pem file.
        # First we rename the .p8 file to .pem then with something like this...
        # >> openssl ec -in apple_api.pem -out apple_api_ec.pem
        # Then we process it to a config file so it doesn't get uploaded to github.
        #
        config_file = 'apple_api_ec.config'
        config_section = 'APPLE_API'
        config = configparser.ConfigParser()
        config.read(config_file)
        if config_section in config:
            # newlines are essential to correct functioning, and the only way to create it properly is thru this script
            self.secret = config[config_section]['SECRET0'] + "\n" + config[config_section]['SECRET1'] + "\n" + config[config_section]['SECRET2']+ "\n" + config[config_section]['SECRET3'] +"\n" + config[config_section]['SECRET4']
            self.keyId = config[config_section]['KEYID']
            self.teamId = config[config_section]['TEAMID']

            alg = 'ES256'
            headers = {
            	"typ": 'JWT',
            	"alg": alg,
            	"kid": self.keyId
            }
            time_now = datetime.now()
            time_expired = datetime.now() + timedelta(hours=12)
            payload = {
            	"iss": self.teamId,
            	"iat": int(time_now.strftime("%s")),
            	"exp": int(time_expired.strftime("%s"))
            }

            self.token = jwt.encode(payload, self.secret, algorithm=alg, headers=headers).decode('UTF-8')
            print('authorized with apple music api...')
        else:
            print('Check {} file for correct section {}'.format(config_file, config.section))

    def request(self, url, cache=CACHE_ENABLED, count=0, last_request=0):
        """
        Request a webpage, retry on failure, cache as desired
        """
        # NOTE: make this file directory smarter. just testing for now.
        cache_file = "./apple/all-%s.json" % date.today().strftime('%m-%d-%Y')

        if count > 3: # retried 3 times, giving up
            print('Failed getting page "%s", retried %i times' % (url, count))
            return False
        if last_request > time.time()-1:
            time.sleep(3) # wait 3 seconds between retries
        try:
            q = Request(url)
            q.add_header('Authorization', 'Bearer {}'.format(self.token))
            data = urlopen(q).read().decode('utf8')
            response = json.loads(data)

            return response
        except HTTPError as err:
            if err.code == 400:
                print('HTTP 400, said:')
            return False
        except Exception as e:
            count += 1
            print('Exception in API request, retrying...')
            # return request(url, cache, count, time.time())
#
#
# APPLE CLASS END

# Data processing and adding to data thru calling API
#
#
def map_kind_to_db_table_name(kind):
    if kind == 'song':
        return 'track'
    elif kind == 'album':
        return 'album'
    elif kind == 'musicVideo':
        return 'music_video'

def get_isrc_by_id(tracks, track_id):
    """
    Return the isrc data for the track matching track_id
    """
    for track in tracks:
        if track['id'] == track_id:
            if 'external_ids' in track and 'isrc' in track['external_ids']:
                return track['external_ids']['isrc']
            else:
                print('isrc data not available for track ID %s' % track_id)
    return False

def get_album_by_id(tracks, track_id):
    """
    Return the album ID for the track matching track_id
    """
    for track in tracks:
        if track['id'] == track_id:
            if 'album' in track and 'id' in track['album']:
                return track['album']['id']
            else:
                print('Album ID not available for track ID %s' % track_id)
    return False

def get_artist_by_id(tracks, track_id):
    """
    Return the artist ID for the track matching track_id
    """
    for track in tracks:
        if track['id'] == track_id:
            if 'artists' in track and track['artists'] and 'id' in track['artists'][0]:
                return track['artists'][0]['id']
            else:
                print('artist ID not available for track ID %s' % track_id)
    return False

def append_apple_id_from_db(items, db_table_name):
    """
    Input:
        tracks: dict
    Output:
        tracks: dict
    Appends key 'apple_id_db' key with db lookup value.
    This removes redundancy from apple API calls to retrieve info already in the DB.
    """
    # NOTE: hardcoded in service_id = 2 for Apple, may want to change to lookup from service table on join
    # service_id = self.get_service_id(service_name)

    query = """
        SELECT id
        FROM {}
        WHERE service_id = 2
        AND service_{}_id = %s
    """.format(db_table_name, db_table_name)


    for apple_id in items:
        if type(apple_id) == int:
            db.c.execute(query, [apple_id])
            row = db.c.fetchone()
            if row:
                items[apple_id]['apple_id_db'] = row[0]
    return items

def append_track_data(items, region):
    """
    Input:
        tracks: dict
        If 'apple_id_db' is not in the item, then it doesn't exist in the db, and we query the Apple API for track data
    Output:
        tracks: dict +
            { 'isrc': xx, 'album_id': xx, 'artist_id': xx} for any track without a 'apple_id_db'
    """
    # apple = Apple()
    endpoint = API_url.format(region=region, media ='songs') + '?ids={}'

    tracks_to_lookup = [apple_id for apple_id, item in items.items() if (type(item) is dict and 'apple_id_db' not in item)]

    if len(tracks_to_lookup) != 0:
        id_str = ','.join(map(str, tracks_to_lookup))
        # retrieve API data and convert to easy lookup format
        r = apple.request(endpoint.format(id_str))
        if r:
            data = r['data']
            r_dict = {item['id']: item for item in data} # construct dictionary with id as key

            for apple_id in r_dict:
                items[apple_id]['isrc'] = (r_dict[apple_id]['attributes']['isrc']).upper()
                items[apple_id]['album_id'] = r_dict[apple_id]['relationships']['albums']['data'][0]['id']
                items[apple_id]['track_genres'] = r_dict[apple_id]['attributes']['genreNames']

            # diagnostics and statistics for printing
            count_new_items = 0
            for apple_id in items:
                if all (key in items[apple_id] for key in ('isrc', 'album_id')):
                    count_new_items += 1
            print('{} new tracks with ISRC and album_id'.format(count_new_items))
        else:
            logger.warn("Region {} songs haven't been looked up by the API. Check for empty isrc, album_id and track_genres for these track ids: {}".format(region, tracks_to_lookup))
            return False # NOTE: new... could cause errors! Make sure to monitor for Boolean
    return items

def append_music_video_data(items, region):
    endpoint = API_url.format(region=region, media ='music-videos') + '?ids={}'
    videos_to_lookup = [apple_id for apple_id, item in items.items() if (type(item) is dict and 'apple_id_db' not in item)]

    if len(videos_to_lookup) != 0:
        id_str = ','.join(map(str, videos_to_lookup))
        # retrieve API data and convert to easy lookup format
        r = apple.request(endpoint.format(id_str))
        if r:
            data = r['data']
            r_dict = {item['id']: item for item in data} # construct dictionary with id as key

            for apple_id in r_dict:
                items[apple_id]['isrc'] = (r_dict[apple_id]['attributes']['isrc']).upper()

            # diagnostics and statistics for printing
            count_new_items = 0
            for apple_id in items:
                if 'isrc' in items[apple_id]:
                    count_new_items += 1
            print('{} new music videos with ISRC'.format(count_new_items))
    else:
        logger.warn("Region {} music videos haven't been looked up by the API. Check for empty isrc for music video ids: {}".format(region, videos_to_lookup))

    return items

def append_album_data(items, region):
    """
    Input:
        items: dict (with 'album_id' key, which refers to apple albumId)
    Output:
        items: dict +
            {'label': xx} for any track with 'albumId' key
    Append the label to items using the Apple albums API
    """
    # apple = Apple()
    endpoint = API_url.format(region=region, media ='albums') + '?ids={}'

    albums_to_lookup = [item['album_id'] for k,item in items.items() if (type(item) is dict and 'album_id' in item)]

    if len(albums_to_lookup) != 0:
        id_str = ','.join(map(str, albums_to_lookup))
        # retrieve API data and convert to easy dict lookup format
        r = apple.request(endpoint.format(id_str))

        if r:
            data = r['data']
            r_dict = {album['id']: album for album in data} # construct dictionary with id as key

            for apple_album_id in r_dict:
                for apple_id, item in items.items():
                    if 'album_id' in item and item['album_id'] == apple_album_id:
                        items[apple_id]['label'] = r_dict[apple_album_id]['attributes']['recordLabel']
                        items[apple_id]['album_release_date'] = r_dict[apple_album_id]['attributes']['releaseDate']
                        items[apple_id]['album_genres'] = r_dict[apple_album_id]['attributes']['genreNames']
                        items[apple_id]['album_name'] = r_dict[apple_album_id]['attributes']['name']
    return items

def append_artist_data(tracks, region):
    """
    Append the genre tags to tracks
    """
    # apple = Apple()
    endpoint = API_url.format(region=region, media ='albums') + '?ids={}'

    artists_to_lookup = [track['artist_id'] for k,track in tracks.items() if 'album_id' in track]

    if len(artists_to_lookup) != 0:
        id_str = ','.join(map(str, albums_to_lookup))

        # retrieve API data and convert to easy lookup format
        r = apple.request(endpoint.format(id_str))
        data = r['data']
        r_dict = {artist['id']: artist for artist in data} # construct dictionary with id as key

        for apple_artist_id in r_dict:
            for apple_track_id, track in tracks.items():
                if 'artist_id' in track and track['artist_id'] == apple_artist_id:
                    tracks[apple_id]['genres'] = r_dict[apple_id]['attributes']['genreNames']
    return tracks

# Process helpers----------
#

# Returns a list of regions
# PARAMS:
#   chart_service is chart[0] in an item of CHARTS
#   kind is chart[1] in an item of  CHARTs
#   url is a processed url from the db
#

def unprocessed_regions(chart_service, kind, date_str = TODAY):
    processed_urls = db.get_processed_by_kind(chart_service, kind, date_str)

    processed_regions = []
    for url in processed_urls:
        processed_regions.append(strip_region_from_url(chart_service, kind, url))

    if chart_service == 'apple-music' and kind == 'top-songs':
        regions = list(set(REGIONS_WITH_APPLE_MUSIC).difference(processed_regions))
    elif chart_service == 'itunes-music'and kind == 'top-albums':
        regions = list(set(REGIONS_WITH_ITUNES_MUSIC_ALBUMS).difference(processed_regions))
    elif chart_service == 'itunes-music'and kind == 'top-songs':
        regions = list(set(REGIONS_WITH_ITUNES_MUSIC_ALBUMS).difference(processed_regions))
    elif chart_service == 'music-videos':
        regions = list(set(REGIONS_WITH_MUSIC_VIDEOS).difference(processed_regions))
    else:
        regions = []
        print('Error in type')

    return regions

# PARAMS:
#   chart_service is chart[0] in an item of CHARTS
#   kind is chart[1] in an item of  CHARTs
#   url is a processed url from the db
#
def strip_region_from_url(chart_service, kind, url):
    start = 'https://rss.itunes.apple.com/api/v1'
    m = re.search('{}/(.*)/{}/{}'.format(start, chart_service, kind), url)
    return m.group(1)

# TrackDatabase class start
#
#
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
            print('Connecting to the PostgreSQL database {}...'.format(rds_host))
            self.db = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name)
            print('Successfully connected to AWS RDS PostgreSQL instance.', rds_host)
            self.db.autocommit = True
            self.c = self.db.cursor()
            self.show_db_size()
            # self.show_table_stats()

        except Exception as e:
            print(e)

    def close_database(self):
        try:
            if self.db:
                self.db.close()
        except:
            print('cannot close db')
        return True

    def show_db_size(self):
        self.c.execute('SELECT pg_size_pretty(pg_database_size(current_database()))')
        size = self.c.fetchone()[0]
        print('Size: {}'.format(size))
        return

    # NOTE: Do not need this for this apple api script - this is playlist data
    def show_table_stats(self):
        self.table_stats = {}
        self.table_stats.setdefault('playlist', {})
        self.table_stats.setdefault('playlist_followers', {})
        self.table_stats.setdefault('playlist_owner', {})
        self.table_stats.setdefault('playlist_track_position', {})

        # TESTING! #
        self.c.execute('SELECT COUNT(*) FROM playlist')
        ts = self.c.fetchone()[0]

        self.table_stats['playlist']['start_count'] = ts
        print("# Playlists Processed: {}".format(ts))

        self.c.execute("SELECT COUNT(*) FROM playlist_followers")
        pfs = self.c.fetchone()[0]
        self.table_stats['playlist_followers']['start_count'] = pfs
        print("# Follower Stats Processed: {}".format(pfs))

        self.c.execute("SELECT COUNT(*) FROM playlist_owner")
        pos = self.c.fetchone()[0]
        self.table_stats['playlist_owner']['start_count'] = pos
        print("# Playlist Owners: {}".format(pos))

        self.c.execute("SELECT COUNT(*) FROM playlist_track_position")
        ptps = self.c.fetchone()[0]
        self.table_stats['playlist_track_position']['start_count'] = ptps
        print("# Track Position Stats: {}".format(ptps))

    def print_region_status_stats(self, date_stats = date.today().strftime('%Y-%m-%d')):
        query_str = '''
            SELECT *
            FROM processed
            WHERE url
            LIKE '%{}%'
        '''

        start = 'https://rss.itunes.apple.com/api/v1/'
        for chart in CHARTS:
            end = '/{}/{}/all/200/explicit.json_{}'.format(chart[0], chart[1], date_stats)
            chart_query = query_str.format(end)
            self.c.execute(chart_query)
            rows = self.c.fetchall()

            regions_processed = []
            for row in rows:
                url = str(row[0])
                region = url.split(start)[1].split(end)[0]
                regions_processed.append(region)
            regions_not_processed = list(set(chart[2]).difference(regions_processed))
            print('-' * 40)
            print('{}:{}: {} processed, {} missing'.format(chart[0], chart[1], len(regions_processed), len(regions_not_processed)))
            print('{}'.format(regions_not_processed))


    def is_processed(self, url):
        """
        Has CSV url already been processed?
        """
        query = """
            SELECT * FROM processed WHERE url = %s
        """
        self.c.execute(query, [url])

        if self.c.fetchone():
            return True
        return False

    def set_processed(self, url):
        """
        Mark url as already processed
        """
        try:
            self.c.execute("""
                INSERT INTO processed
                (url)
                VALUES
                (%s)
            """, (url,))

            self.db.commit()

        except psycopg2.IntegrityError as e:
            print('Integrity error: {}'.format(e))
        return True

    # helper functions
    def get_track_stats(self, service_id, territory_id, track_id):
        """
        Returns a tuple of track stats (track_id, territory_id, service_id, added, last_seen, peak_rank, peak_date)
        """
        query = self.c.execute("""
            SELECT
                id,
                peak_rank,
                peak_date
            FROM track_position_peak
            WHERE
                service_id = %s
            AND
                territory_id = %s
            AND
                track_id = %s
            """,
            (service_id, territory_id, track_id)
        )
        row = self.c.fetchone()
        return row if row else False

    def get_sales_stats(self, service_id, territory_id, media_id, media_type):
        """
        Returns a tuple of track stats (apple_id_db, media_type, territory_id, service_id, added, last_seen, peak_rank, peak_date)
        media_id and media_type are a combined "foreign key", defining the table 'track', and the 'track_id', etc.
        """
        query = self.c.execute("""
            SELECT
                id,
                peak_rank,
                peak_date
            FROM sales_position_peak
            WHERE
                service_id = %s
                AND territory_id = %s
                AND media_id = %s
                AND media_type = %s
            """,
            (service_id, territory_id, media_id, media_type)
        )
        row = self.c.fetchone()
        return row if row else False

    def order_dates(self, a, b):
        """
        Order two date strings (YYYY-MM-DD) chronologically
        Returns a tuple of two dates, earliest first
        """
        ymd_regex = r'^(\d{4})-(\d{2})-(\d{2})$'
        a_matches = re.search(ymd_regex, a)
        b_matches = re.search(ymd_regex, b)
        if not a_matches and b_matches:
            return False
        assert a_matches and b_matches, 'Invalid date strings supplied to "order_dates"'
        if a_matches.group(1) < b_matches.group(1):
            return (a, b)
        if a_matches.group(2) < b_matches.group(2):
            return (a, b)
        if a_matches.group(3) < b_matches.group(3):
            return (a, b)
        return (b, a)

    def get_isrc_from_db(self, db_id, db_table):
        # RETRIEVE ISRC
        query = """
            SELECT isrc
            FROM {}
            WHERE id = %s
        """.format(db_table)

        db.c.execute(query, [db_id])
        row = db.c.fetchone()
        return row[0] if row else False

    def get_territory_id(self, code):
        """
        Retrieve territory_id from region code
        """
        self.c.execute("""
            SELECT id FROM territory WHERE code = %s
        """, [code.lower()])

        row = self.c.fetchone()
        return row[0] if row else False

    def get_service_id(self, service_name):
        """
        Retrieve service_id from service name
        """
        self.c.execute("""
            SELECT id FROM service WHERE service_name = %s
        """, [service_name])

        row = self.c.fetchone()
        return row[0] if row else False

    def get_artist_id(self, service_id, service_artist_id):
        """
        Retrive artist id
        """
        self.c.execute("""
            SELECT id FROM artist
            WHERE
            service_id = %s
            AND
            service_artist_id = %s
        """, (service_id, service_artist_id)
        )

        row = self.c.fetchone()
        return row[0] if row else False

    def get_album_id(self, service_id, service_album_id):
        """
        Retrive album id
        """
        self.c.execute("""
            SELECT id FROM album
            WHERE
            service_id = %s
            AND
            service_album_id = %s
        """, (service_id, service_album_id)
        )
        row = self.c.fetchone()
        return row[0] if row else False

    def get_music_video_id(self, service_id, service_music_video_id):
        """
        Retrive music video id
        """
        self.c.execute("""
            SELECT id FROM music_video
            WHERE
            service_id = %s
            AND
            service_music_video_id = %s
        """, (service_id, service_music_video_id)
        )
        row = self.c.fetchone()
        return row[0] if row else False


    def get_processed_by_kind(self, chart_service, kind, date_str):
        """
        Retrieve the list of processed urls for a type (album, song, music-video) and date
        """
        query = "SELECT url from processed WHERE url like '%{}/{}/all/200/explicit.json_{}%'".format(chart_service, kind, date_str)

        print('QUERY IS:', query)

        self.c.execute(query)

        rows = self.c.fetchall()

        print('get processed by kind rows', rows)

        urls = []
        for row in rows:
            urls.append(row[0])
        print('urls', urls)
        return urls

    # update stats functions
    # TODO: test to see if they work ;)
    #
    def update_track_stats(self, service_id, territory_id, track_id, isrc, position, date_str):
        """
        Update the rolling stats for a track
        """

        # latest track stats in the db
        position = int(position)
        stats = self.get_track_stats(service_id, territory_id, track_id)

        if stats:
            track_stat_id, peak_rank, peak_date = stats

        stats_update_query = """
            UPDATE track_position_peak SET
                peak_rank = %s,
                peak_date = %s
            WHERE
                id = %s
        """

        stats_query = """
            INSERT INTO track_position_peak
            (service_id, territory_id, track_id, isrc, peak_rank, peak_date)
            VALUES
            (%s, %s, %s, %s, %s, %s)
        """
        if stats:
            if position < peak_rank:
                self.c.execute(
                    stats_update_query,
                    (position, date_str, track_stat_id)
                )
        # track doesn't have existing stats
        else:
            self.c.execute(
                stats_query,
                (service_id, territory_id, track_id, isrc, position, date_str)
            )

        self.db.commit()

    def update_sales_stats(self, service_id, territory_id, media_id, media_type, position, date_str):
        # latest track stats in the db
        position = int(position)
        stats = self.get_sales_stats(service_id, territory_id, media_id, media_type)

        if stats:
            sales_stat_id, peak_rank, peak_date = stats

        stats_update_query = """
            UPDATE sales_position_peak SET
                peak_rank = %s,
                peak_date = %s
            WHERE
                id = %s
        """

        stats_query = """
            INSERT INTO sales_position_peak
            (service_id, territory_id, media_id, media_type, peak_rank, peak_date)
            VALUES
            (%s, %s, %s, %s, %s, %s)
        """

        if stats:
            if position < peak_rank:
                self.c.execute(
                    stats_update_query,
                    (position, date_str, sales_stat_id)
                )
        # track doesn't have existing stats
        else:
            self.c.execute(
                stats_query,
                (service_id, territory_id, media_id, media_type, position, date_str)
            )

    # helper functions to add individual items to tables
    def add_artist(self, service_id, service_artist_id, artist_name):
        self.c.execute("""
            INSERT INTO artist
            (service_id, service_artist_id, artist)
            VALUES
            (%s, %s, %s)
            RETURNING id
        """, (service_id, service_artist_id, artist_name))

        db_artist_id = self.c.fetchone()[0]
        print('Artist {} added: {}'.format(db_artist_id, artist_name))
        return db_artist_id

    def add_album(self, service_id, artist_id, service_album_id, album_name, release_date, label):
         self.c.execute("""
             INSERT INTO album
             (service_id, artist_id, service_album_id, album, release_date, label)
             VALUES
             (%s, %s, %s, %s, %s, %s)
             RETURNING id
         """, (service_id, artist_id, service_album_id, album_name, release_date, label)
         )
         db_album_id = self.c.fetchone()[0]
         print('Album {} added: {} by artist id {}'.format(db_album_id, album_name, artist_id))
         return db_album_id

    def add_track(self, service_id, track_id, artist_id, album_id, track_name, isrc):
        self.c.execute("""
            INSERT INTO track
            (service_id, service_track_id, artist_id, album_id, track, isrc)
            VALUES
            (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """,
            (service_id, track_id, artist_id, album_id, track_name, isrc)
        )
        db_track_id = self.c.fetchone()[0]
        print('Track {} added: {} by artist id {} with ISRC {}'.format(db_track_id, track_name, artist_id, isrc))

        return db_track_id

    def add_artist_genre(self, service_id, artist_id, genre):
        self.c.execute("""
            INSERT INTO artist_genre
            (service_id, artist_id, genre)
            VALUES
            (%s, %s, %s)
        """, (service_id, artist_id, genre))

    def add_music_video(self, service_id, service_music_video_id, artist_id, music_video_name, release_date, isrc):
        self.c.execute("""
            INSERT INTO music_video
            (service_id, service_music_video_id, artist_id, music_video, release_date, isrc)
            VALUES
            (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (service_id, service_music_video_id, artist_id, music_video_name, release_date, isrc)
        )
        db_mv_id = self.c.fetchone()[0]

        print('Music video {} added: {}, by artist id {}'.format(db_mv_id, music_video_name, artist_id))
        return db_mv_id

    # main functions to add data in item lists to database
    def add_track_items(self, items, date_str, region, service_name):
        """
        input:
            items: dict of all songs to add
        Add tracks to the database
        """
        service_id = self.get_service_id(service_name)
        territory_id = self.get_territory_id(region)
        db_table = items['db_table_name']
        media = items['media']

        for track_id, item in items.items():
            if type(item) is dict:
                position = item['position']

                if 'apple_id_db' in item:
                    track_id = item['apple_id_db']
                    isrc = self.get_isrc_from_db(track_id, db_table)

                # item isn't in DB, other attributes must be added to db
                else:
                    try:
                        # Map from RSS/API response to variables
                        #
                        artist_name = str(item['artistName'])
                        service_artist_id = str(item['artistId'])
                        release_date = str(item['releaseDate'])
                        track_name = str(item['name'])
                        album_name = str(item['collectionName'])
                        service_track_id = str(item['id'])

                        if 'album_id' in item:
                            service_album_id = str(item['album_id'])

                            if db_table == 'music_video':
                                service_album_id = ''
                        else:
                            # NOTE: not sure about this...
                            service_album_id = ''

                        # Lookup Artist, Album or Track db ids
                        #
                        artist_id = self.get_artist_id(service_id, service_artist_id)
                        album_id = self.get_album_id(service_id, service_album_id)


                        # Override album name if API name is different than the RSS feed
                        if ('album_name' in item) and (item['album_name'] != album_name):
                            album_name = item['album_name']

                        # Assign metadata to variables if they exist
                        #
                        # albums don't have isrc
                        if 'isrc' in item:
                            isrc = str(item['isrc'])
                        else:
                            isrc = ''
                            logger.warn('No ISRC for apple {} id {}'.format(db_table, item['id']) )

                        # Note: Music videos don't have a label.
                        if 'label' in item:
                            label = str(item['label'])
                        else:
                            label = ''
                            logger.warn('No label for apple {} id {}'.format(db_table, item['id']) )

                        if 'album_genres' in item:
                            genres = item['album_genres']
                        else:
                            # get it from the RSS feed response
                            genres = [g['name'] for g in item['genres']]


                        earliest_release_date = release_date
                        if 'album_release_date' in item:
                            album_release_date = item['album_release_date']
                            ordered_dates = self.order_dates(album_release_date, release_date)

                            if ordered_dates:
                                earliest_release_date = ordered_dates[0]

                            if (release_date != album_release_date):
                                logger.debug('Release date inconsistency: Apple track id: {}, artist: {}, Apple album id: {}, track release date {}, and album release date {}'.format(service_track_id, artist_name, service_album_id, release_date, album_release_date))

                        # TODO: test if genres are consistent among artist, album and track
                        # and from RSS and api responses.
                        #
                        # if (collectionName != album_name):
                        #     logger.debug('Album name inconsistency: {}, {}'.format(collectionName, album_name))


                        # Update Database
                        #
                        # add artist if not in the db
                        if not artist_id:
                            artist_id = self.add_artist(service_id, service_artist_id, artist_name)

                        # add album if not in the db
                        if not album_id:
                            album_id = self.add_album(service_id, artist_id, service_album_id, album_name, release_date, label)

                        # add genres for artist
                        for genre in genres:
                            self.add_artist_genre(service_id, artist_id, genre)

                        # update track table
                        track_id = self.add_track(service_id, track_id, artist_id, album_id, track_name, isrc)

                    except Exception as e:
                        print(media, db_table, e)
                        return False
                        raise

                if media == 'apple-music':
                    self.update_track_stats(service_id, territory_id, track_id, isrc, position, date_str)

                    self.c.execute("""
                        INSERT INTO track_position
                        (service_id, territory_id, track_id, isrc, position, date_str)
                        VALUES
                        (%s, %s, %s, %s, %s, %s)
                    """, (service_id, territory_id, track_id, isrc, position, date_str)
                    )
                elif media == 'itunes-music':
                    self.update_sales_stats(service_id, territory_id, track_id, 'track', position, date_str)

                    self.c.execute("""
                        INSERT INTO sales_position
                        (service_id, territory_id, media_id, media_type, position, date_str)
                        VALUES
                        (%s, %s, %s, %s, %s, %s)
                    """, (service_id, territory_id, track_id, 'track', position, date_str)
                    )


        return True

    def add_album_items(self, items, date_str, region, service_name):
        service_id = self.get_service_id(service_name)
        territory_id = self.get_territory_id(region)
        media = items['media']

        for service_album_id, item in items.items():
            if type(item) is dict:
                position = item['position']

                if 'apple_id_db' in item:
                    album_id = item['apple_id_db']

                # item isn't in DB, other attributes must be added to db
                else:
                    try:
                        # Map from RSS/API response to variables
                        #
                        artist_name = str(item['artistName'])
                        service_artist_id = str(item['artistId'])
                        release_date = str(item['releaseDate'])
                        album_name = str(item['name'])

                        if 'label' in item:
                            label = str(item['label'])
                        else:
                            label = ''
                            logger.warn('No label for apple album id {}'.format(service_album_id) )

                        genres = [g['name'] for g in item['genres']]

                        # Lookup Artist, Album or Track db ids
                        #
                        artist_id = self.get_artist_id(service_id, service_artist_id)
                        album_id = self.get_album_id(service_id, service_album_id) # NOTE: do I need this? doesn't 'apple_id_db' take care of this check for me? YES!

                        # add artist if not in the db
                        if not artist_id:
                            artist_id = self.add_artist(service_id, service_artist_id, artist_name)

                        # add album if not in the db
                        if not album_id:
                            album_id = self.add_album(service_id, artist_id, service_album_id, album_name, release_date, label)

                        # add genres for artist
                        for genre in genres:
                            self.add_artist_genre(service_id, artist_id, genre)

                    except Exception as e:
                        print(e)
                        raise

                if media == 'itunes-music':
                    self.update_sales_stats(service_id, territory_id, album_id, 'album', position, date_str)

                    self.c.execute("""
                        INSERT INTO sales_position
                        (service_id, territory_id, media_id, media_type, position, date_str)
                        VALUES
                        (%s, %s, %s, %s, %s, %s)
                    """, (service_id, territory_id, album_id, 'album', position, date_str)
                    )

        return True

    def add_music_video_items(self, items, date_str, region, service_name):
        service_id = self.get_service_id(service_name)
        territory_id = self.get_territory_id(region)
        db_table = items['db_table_name']
        media = items['media']

        for service_music_video_id, item in items.items():
            if type(item) is dict: # NOTE: if it's a dict, it is not an added key for metadata. Should probably find another solution then this (either don't add extra keys and pass extra params around, or put it in another data structure)
                position = item['position']

                if 'apple_id_db' in item:
                    music_video_id = item['apple_id_db']
                    isrc = self.get_isrc_from_db(music_video_id, db_table)

                # item isn't in DB, other attributes must be added to db
                else:
                    try:
                        # Map from RSS/API response to variables
                        #
                        artist_name = str(item['artistName'])
                        service_artist_id = str(item['artistId'])
                        release_date = str(item['releaseDate'])
                        music_video_name = str(item['name'])

                        # Lookup Artist, Album or Track db ids
                        #
                        artist_id = self.get_artist_id(service_id, service_artist_id)
                        music_video_id = self.get_music_video_id(service_id, service_music_video_id) # NOTE: can re-use the append_track_id_from_db

                        # Assign metadata to variables if they exist
                        #
                        # albums don't have isrc
                        if 'isrc' in item:
                            isrc = str(item['isrc'])
                        else:
                            isrc = ''
                            logger.warn('No ISRC for apple {} id {}'.format(db_table, item['id']) )

                        genres = [g['name'] for g in item['genres']]

                        # Update Database
                        #
                        # add artist if not in the db
                        if not artist_id:
                            artist_id = self.add_artist(service_id, service_artist_id, artist_name)

                        if not music_video_id: # NOTE: does the logic need to re-check the music_video_id (using self.get_music_video_id) just to find if it doesn't exist? Can we just check to see if it's in locals() or set it to false or empty
                            music_video_id = self.add_music_video(service_id, service_music_video_id, artist_id, music_video_name, release_date, isrc)

                        # add genres for artist
                        for genre in genres:
                            self.add_artist_genre(service_id, artist_id, genre)

                    except Exception as e:
                        print(e)
                        raise

                if media == 'music-videos':
                    self.update_sales_stats(service_id, territory_id, music_video_id, 'music_video', position, date_str)

                    self.c.execute("""
                        INSERT INTO sales_position
                        (service_id, territory_id, media_id, media_type, position, date_str)
                        VALUES
                        (%s, %s, %s, %s, %s, %s)
                    """, (service_id, territory_id, music_video_id, 'music_video', position, date_str)
                    )

        return True

def process(charts, regions):
    """
    Process each region
    """

    global db # for the try/except block to reset db if it for some reason doesn't exist

    try:
        print('*' * 40)
        db.print_region_status_stats()
        print('*' * 40)

        service_name = 'Apple'
        number_results = 50
        limit = '&limit={}'.format(number_results)

        starttime_total = datetime.now() # timestamp

        for chart in charts:

            # set rss url params
            #
            rss_params = {
                'region': '',
                'media': chart[0],
                'chart': chart[1],
                'genre': 'all',
                'limit': 200,
            }

            for region in regions:
                starttime = datetime.now() # timestamp
                print('Starting processing at', starttime.strftime('%H:%M:%S %m-%d-%y')) # timestamp

                # set variables
                rss_params['region'] = region
                url = RSS_url.format(**rss_params)

                try:
                    req = Request(url)
                    r = urlopen(req).read().decode('UTF-8')
                except HTTPError as err:
                    if err.code == 400:
                        print('HTTP 400 with:', url)
                        continue
                    if err.code == 404:
                        print('No RSS feed data found for {} for {} in {}'.format(region, rss_params['media'], rss_params['chart']))
                        logger.warn('No RSS feed data found for {} for {} in {}'.format(region, rss_params['media'], rss_params['chart']))
                        print('-' * 40)
                        continue
                except URLError as e:
                    print('We failed to reach a server. URLError with:', url, e.reason)

                # Process the data
                print('Loading {} {} charts for region {}...'.format(rss_params['media'], rss_params['chart'], region))

                try:
                    raw_data = json.loads(r)
                    results = raw_data['feed']['results']
                    date_str = parser.parse(raw_data['feed']['updated']).strftime('%Y-%m-%d')
                    print('Chart last updated: {}'.format(date_str))

                except ValueError:
                    print('Decoding JSON failed')
                    print('No download available, skipping...')
                    print('-' * 40)
                    continue

                # check if region has been processed, if so, move to the next region in the loop
                if db.is_processed(url + '_' + date_str):
                    print('Already processed, skipping...')
                    print('-' * 40)
                    continue

                # the kind of resource from Apple RSS feed
                # kind is 'song', 'album', 'musicVideo' - labels from Apple's feed
                kind = results[0]['kind']
                db_table_name = map_kind_to_db_table_name(kind)

                # init variables
                items = {}
                items['kind'] = kind
                items['db_table_name'] = db_table_name
                items['media'] = chart[0]
                items['chart'] = chart[1]

                # append position based on list index
                # convert list to dictionary for easier lookup, key is apple id
                for i, result in enumerate(results):
                    result['position'] = i + 1
                    apple_id = result['id']
                    items[apple_id] = result

                    # add album_id key for consistency with appending it in the track items dict
                    if kind == 'album':
                        items[apple_id]['album_id'] = result['id']

                print('Found {} results for {}, {}.'.format(len(results), chart[0], chart[1]))

                # append data to Apple data
                print('Looking up existing ids in db')
                items = append_apple_id_from_db(items, db_table_name)

                complete = False
                if kind == 'song':
                    print('Getting track data from Apple "Tracks" API...')
                    items = append_track_data(items, region)

                    if items:
                        print('Getting label and release date from Apple "Albums" API...')
                        items = append_album_data(items, region)
                    if items:
                        complete = db.add_track_items(items, date_str, region, service_name)
                elif kind == 'album':
                    print('Getting label and release date from Apple "Albums" API...')
                    items = append_album_data(items, region)
                    if items:
                        complete = db.add_album_items(items, date_str, region, service_name)
                elif kind == 'musicVideo':
                    items = append_music_video_data(items, region)
                    if items:
                        complete = db.add_music_video_items(items, date_str, region, service_name)



                # NOTE: no need to append genres here. They exist in RSS feed response
                # print('Getting genre tags from Apple "Artists" API...')
                # items = append_artist_data(items, region)

                # write data to DB
                if complete:
                    db.set_processed(url + '_' + date_str)

                # timestamp
                endtime = datetime.now()
                processtime = endtime - starttime
                processtime_running_total = endtime - starttime_total
                print('Finished processing at', endtime.strftime('%H:%M:%S %m-%d-%y'))
                print('Processing time: %i minutes, %i seconds' % divmod(processtime.days * 86400 + processtime.seconds, 60))
                print('Running processing time: %i minutes, %i seconds' % divmod(processtime_running_total.days *86400 + processtime_running_total.seconds, 60))
                print('Finished Apple API for {}'.format(region))
                print('-' * 40)

        # timestamp
        endtime_total = datetime.now()
        processtime_total = endtime_total - starttime_total
        print('Finished processing all regions at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
        print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days * 86400 + processtime_total.seconds, 60))
        print('-' * 40)

        # no data
        print('Rerun these regions for these charts:')
        print('+' * 40)
        db.print_region_status_stats()

    except psycopg2.InterfaceError as e:
        print('InterfaceError: ', e)
        print('Reconnecting...')
        db = TrackDatabase()

#--------------
# AWS Lambda Handler
#

def get_unprocessed_apple_regions(event, context):
    global db
    db = TrackDatabase()

    # NOTE: Possible refactor: could use the chartIndex like in the handler()... pros and cons

     # in serverless.yml, user can input date_str
    if event.setdefault('date_str', None):
        regions = unprocessed_regions(event['chart_service'], event['kind'], event['date_str'])
    else:
        regions = unprocessed_regions(event['chart_service'], event['kind'])

    db.close_database()
    print('closed database connection')
    return regions

def handler(event, context):
    # Primer on AWS Lambda container reuse
    # https://stackoverflow.com/questions/43355278/aws-lambda-rds-mysql-db-connection-interfaceerror
    # https://aws.amazon.com/blogs/compute/container-reuse-in-lambda/
    global db
    global apple

    db = TrackDatabase()
    apple = Apple()

    # Initialize input variables
    # event = { chartIndex: 0 ...}
    chartIndex = event['chartIndex'] # pass in index for chunking the processing
    if chartIndex >= 0 and chartIndex <= 3: # ensure santizied input
        charts = [CHARTS[chartIndex]] # recreate array structure of CHARTS

    # event = {'regions': ['us', 'ab', '<ETC>'] ...}
    regions = event['regions']

    print('processing : ', charts[0][0], charts[0][1], regions)

    # do work
    process(charts, regions)

    # clean up
    db.close_database()
    print('closed database connection')
    return 'finished'
