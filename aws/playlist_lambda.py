import sys
sys.path.insert(0, './aws_packages') # local relative path of aws lambda packages for zipping

import csv, codecs, re, json, os, base64, time, hashlib, ssl, datetime, logging, errno, random
from pprint import pprint as pprint
from datetime import datetime, date, timedelta
from socket import error as SocketError
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from retrying import retry
import psycopg2

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logger.setLevel(logging.DEBUG)

PRINT_PREFIX = '++'

SERVICE_ID = 1

# TODAY is day in UTC - 8hrs, or PST
# https://julien.danjou.info/blog/2015/python-and-timezones
TODAY = (datetime.utcnow() - timedelta(hours=8)).strftime('%Y-%m-%d')

# cache http requests?
CACHE_ENABLED = False

# spotify app client ID
CLIENT_ID = 'e021413b59f5430d9b1b0b46f67c9dec'

# spotify app client secret
CLIENT_SECRET = '1c155d57d1514944972ea4a6b7ed7554'

SPOTIFY_API = 'https://api.spotify.com/v1'

# Spotify Users Playlists
#
SPOTIFY_USERS = ['spotify', 'topsify', 'filtr']
UNIVERSAL_USERS = ['radioactivehits', 'digster.ee', 'digster.dk',  'digster.co.uk', 'hhheroes',
'digster.lt', 'capitolchristianmusicgroup', 'capitolrecords', 'digsterca', '11145233736', '12150271040',
'digsterdeutschland', 'digstercz', 'digster.lv', 'hollywdrecrds', 'record_club_umc',
'sozoofficial', '116734391', 'digsterhu', 'getmusicasia', 'disney_pixar_', 'digstersk', 'deutschegrammophon', '11152361853',
'100keepit', 'universal.fm', 'digsternl', '12150809594', 'thisisofficial', 'universalmusicargentina', 'universalmusicse',
'udiscover', 'umusicnz', 'universalmusicitalia', 'progrocksmusic', 'thecompletecollection', 'digsterargentina', 'abbaspotify',
'defjamrecordings', 'digster.fr', 'digsterno', 'digster.au', '100yearsoftheblues', 'universal.pictures.de', 'o.owner_id', '128899670',
'digstergreece', 'universalmusica', 'digster.fi', 'digster.se', 'universalmusictaiwan', 'classicmotownrecords',
'digster_italy', 'digster_brasil', 'thejazzlabels', 'universalmusicireland', 'wowilovechristianmusic',  'digster.fm',
'digsterchile', 'disney_music_uk', 'udiscovermusic', 'universal_music_rock_legends', 'digster.pt']
ALL_USERS = SPOTIFY_USERS + UNIVERSAL_USERS

# list to use
# USERS = ALL_USERS
# USERS = SPOTIFY_USERS
USERS = ['topsify']

# other lists TO REMOVE
MESSEDUP_UNIVERSAL_USERS = ['el_listÃ³n', 'digstertÃ¼rkiye']
CLASSICAL_PLAYLIST_USERS = ['sinfinimusic.nl', 'sinfinimusic', 'peacefulclassics', 'dgdeccaclassics']

# current language playlists
# Playlists where name like '%Learn' and owner_id = 9 (or Spotify's owner id)
# LANGUAGE_PLAYLIST_DB_IDS = [1153, 1162, 1163, 1164, 1165, 1166, 1167, 1168, 1169, 1170, 960, 1006, 978, 961, 962, 984] # NOTE: added in from 960 onwards manually, since function was getting stuck
UNWANTED_PLAYLIST_SPOTIFY_IDS = [
    # orchestra/classical
    '37i9dQZF1DX4sWSpwq3LiO', '37i9dQZF1DWYi4w24l7FTx', '37i9dQZF1DXah8e1pvF5oE', '37i9dQZF1DXddGd6mP5X2a',
    '37i9dQZF1DXameWHxm60IU',
    '37i9dQZF1DXc6li3e9oatQ', '37i9dQZF1DX1QCg8MO15wF', '37i9dQZF1DWTJSgpZmw7H2', '37i9dQZF1DWVrSKB2Pc3PY', '37i9dQZF1DWW6K9D6JN1rY', '37i9dQZF1DX0yHwYvqyUJQ', '37i9dQZF1DX2SgxzTVd6bU', '37i9dQZF1DX60lVXkfYly8', '37i9dQZF1DWSsCx004HXRd', '37i9dQZF1DWSIZLZz4Kogf']


# max number of times to retry http requests
MAX_url_RETRIES = 10

# seconds to wait between retry attempts
SECONDS_BETWEEN_RETRIES = 3

# unverified SSL context
SSL_CONTEXT = ssl._create_unverified_context()

# Spotify has a 50 maximum request on return from API
MAX_LIMIT_QUERY = 50

def get_page(url, count=0, last_request=0, return_full=False):
    """
    Request a webpage, retry on failure, cache as desired
    """
    if count > MAX_url_RETRIES:
        print('Failed getting page "%s", retried %i times' % (url, count))
        logger.warning('Failed getting page {}, retried {} times'.format(url, count))
        return False
    if last_request > time.time()-1:
        time.sleep(SECONDS_BETWEEN_RETRIES)
    try:
        r = urlopen(url.encode('utf-8'), context=SSL_CONTEXT)
        return r if return_full else r.read().decode('utf-8')
    except Exception as e:
        count += 1
        print('error: ', e)
        print('Failed getting url "%s", retrying...' % url)
        return get_page(url, count, time.time(), return_full)

def load_spotify_csv_data(region, date='latest'):
    """
    Load and process the CSV file for a given region and date
    Returns a dictionary of tracks with key of spotify track ID
    and with region and track ID appended
    """
    """ Example return data:
    {'2SmgFAhQkQCQPyBiBxR6Te':
        {'Position': '1', 'Track Name': 'Criminal',
        'Artist': 'Natti Natasha',
        'Streams': '267879',
        'URL': 'https://open.spotify.com/track/2SmgFAhQkQCQPyBiBxR6Te',
        'region': 'ar', 'trackId': '2SmgFAhQkQCQPyBiBxR6Te'},
    '<id>':
        {'etc': 'etc' }
    }
    """
    url = get_spotify_csv_url(region, date)
    r = get_page(url, return_full=True) # SPOTIFY CSV is 'r'
    info = r.info()
    if info.get_content_type() != 'text/csv':
        return False
    else:
        spotify_csv = codecs.iterdecode(r, 'utf-8')
    # NOTE: concatenate r to master csv file.
        # Add in csv field for territory 2-letter code, OR territory_id, or a dictionary lookup
    # NOTE: Refactor:
    rows = csv.reader(codecs.iterdecode(r, 'utf-8'))
    fields = None
    data = {}
    for row in rows:
        if not fields:
            fields = row
            continue
        track = dict(zip(fields, row))
        if len(track) == len(fields):
            track['region'] = region
            if track['URL']: # once in awhile the feed URL is empty, check to make sure, don't process if it is
                track['trackId'] = get_track_id_from_url(track['URL'])

                # set key value of dictionary to be spotify track id
                data[track['trackId']] = track
            else:
                continue
    return data

# SPOTIFY CLASS START
#
#
class Spotify(object):
    """ Handle Basic Spotify OAuth2 Requests """
    def __init__(self, client_id, client_secret):
        super(Spotify, self).__init__()
        self.client_id = client_id
        self.client_secret = client_secret
        self.expires = 0
        self.authorize()
    def authorize(self):
        """
        Use credentials to get access token
        """
        endpoint = 'https://accounts.spotify.com/api/token'
        auth = base64.b64encode(bytes('%s:%s' % (self.client_id, self.client_secret), 'utf-8'))
        data = urlencode({
            'grant_type': 'client_credentials'
        }).encode('UTF-8')
        r = Request(endpoint, data)
        r.add_header('Authorization', 'Basic %s' % auth.decode('UTF-8'))
        response = urlopen(r, context=SSL_CONTEXT).read()
        auth_dict = json.loads(response)
        self.access_token = auth_dict['access_token']
        self.token_type = auth_dict['token_type']
        self.expires = int(time.time()+auth_dict['expires_in'])
    def get_token(self):
        """
        Return access token
        Requests a new token if the last one is expired
        """
        if self.expires <= int(time.time()):
            self.authorize()
        return self.access_token

    @retry(
        retry_on_exception=(IOError,),
        stop_max_attempt_number=3,
        wait_fixed=1500)
    def request(self, url, cache=CACHE_ENABLED, count=0, last_request=0):
        """
        Request a webpage, retry on failure, cache as desired
        """
        hashedurl = hashlib.sha256(url.encode('utf-8')).hexdigest()
        cache_file = "./cache/{}.cache".format(hashedurl)
        if cache and os.path.isfile(cache_file):
            with open(cache_file) as f:
                # return cached json
                data = f.read()
                return json.loads(data)
        if count > 3:
            # retried 3 times, giving up
            print('Failed getting page "{}", retried {} times'.format(url, count))
            return False
        if last_request > time.time()-1:
            # wait 3 seconds between retries
            time.sleep(3)

        # make request
        headers = {
            'Authorization': 'Bearer {}'.format(self.get_token()),
            'Accept': 'application/json',
            'User-agent': 'your bot 0.{}'.format(random.randint(1,101))
        }

        if cache and not os.path.exists(os.path.dirname(cache_file)):
            try:
                os.makedirs(os.path.dirname(cache_file))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        try:
            q = Request(url, None, headers)
            data = urlopen(q, context=SSL_CONTEXT).read().decode('utf-8')
            if cache:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    f.write(data)
            return json.loads(data)

        except URLError as e:
            print('request issue! URLError in the request... ', str(e.reason), ' for ', url)
            print('retry after is', e.headers, e.headers['Retry-After'])
            # return False
        except ConnectionResetError as e:
            print('request issue! Error 54: Connection reset error. ', str(e))
            return get_page(url, cache, count, time.time())
        except HTTPError as err:
            if err.code == 400:
                print('request issue! HTTP400 error... {}'.format(url))
                return False
            elif err.code == 401:
                print('request issue! HTTP401 error... {}'.format(url))
                return False
        except SocketError as e:
            if e.errno != errno.ECONNRESET:
                raise # Not error we are looking for
            pass # Handle error here.
        except Exception as e:
            count += 1
            return get_page(url, cache, count, time.time())
#
# SPOTIFY CLASS END

# GET IDS AND DATA FROM API METHODS
#
def get_isrc_by_id(tracks, track_id):
    """
    Return the isrc data for the track matching track_id
    """
    for track in tracks:
        if track['id'] == track_id:
            if 'external_ids' in track and 'isrc' in track['external_ids']:
                return track['external_ids']['isrc']
            else:
                print('ISRC data not available for track ID %s' % track_id)
                logger.warning('ISRC data not available for track ID {}'.format(track_id))
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
                logger.warning('Album ID not available for track ID {}'.format(track_id))
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
                logger.warning('artist ID not available for track ID {}'.format(track_id))
    return False

def append_track_artist_album_db_ids(tracks):
    """
    Input:
        tracks: dict
    Output:
        tracks: dict
    This removes redundancy from spotify API calls to retrieve info already in the DB.
    """
    # NOTE: hardcoded in service_id = 1 for Spotify, may want to change to lookup
    # service_id = self.get_service_id(service_name)
    if tracks:
        for track_id, track in tracks.items():
            tracks[track_id] = append_db_track_id(track)
            tracks[track_id] = append_db_artist_id(track)
            tracks[track_id] = append_db_album_id(track)

    return tracks

def append_db_track_id(track):
    track.setdefault('db_track_id', None)

    query = """
        SELECT id
        FROM track
        WHERE service_id = 1
        AND service_track_id = %s
    """
    # NOTE: there should be a one-to-one relationship between spotify trackId and db id
    db.c.execute(
        query,
        [track['track_id']]
    )

    row = db.c.fetchone()

    if row:
        track['db_track_id'] = row[0]

    return track

def append_db_artist_id(track):
    track.setdefault('db_artist_id', None)

    query = """
        SELECT id
        FROM artist
        WHERE service_id = 1
        AND service_artist_id = %s
    """
    # NOTE: there should be a one-to-one relationship between spotify trackId and db id
    db.c.execute(query, [track['artist_id']])
    row = db.c.fetchone()

    if row:
        track['db_artist_id'] = row[0]

    return track

def append_db_album_id(track):
    track.setdefault('db_album_id', None)

    query = """
        SELECT id
        FROM album
        WHERE service_id = 1
        AND service_album_id = %s
    """
    # NOTE: there should be a one-to-one relationship between spotify trackId and db id
    db.c.execute(query, [track['album_id']])
    row = db.c.fetchone()

    if row:
        track['db_album_id'] = row[0]

    return track

def append_album_label_release_data(tracks, batch_size=20):
    """
    Input:
        tracks: dict (with 'albumId' key, which refers to spotify albumId)
    Output:
        tracks: dict +
            { 'release_date': xx, 'label': xx} for any track with 'albumId' key
    Append the label and release date to tracks using the Spotify albums API
    See: https://developer.spotify.com/web-api/console/get-several-albums/
    Returns tracks with "label" and "released" appended
    """
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)
    endpoint_album = "https://api.spotify.com/v1/albums/{}"
    endpoint_albums = "https://api.spotify.com/v1/albums?ids={}"

    # api supports up to 20 ids at a time
    albums = [t['album_id'] for k,t in tracks.items() if not t['db_album_id']]

    if len(albums) != 1:
        batches = [albums[i:i + batch_size] for i in range(0, len(albums), batch_size)]

        r_list = []
        for i, batch in enumerate(batches):
            # remove None values from batch
            while True:
                try:
                    batch.remove(None)
                except:
                    break

            id_str = ','.join(batch)
            r = spotify.request(endpoint_albums.format(id_str))
            r_list += r['albums']
            print('Retrieved album data {} of {} batches'.format(i + 1, len(batches)))

        album_data_dict = convert_list_to_dict_by_attribute(r_list, 'id')

        for track_id, track in tracks.items():
            if not track['db_album_id']:
                tracks[track_id]['album_release_date'] = album_data_dict[track['album_id']]['release_date']
                tracks[track_id]['album_label'] = album_data_dict[track['album_id']]['label']

    else:
        album_id = albums[0]
        album = spotify.request(endpoint_album.format(album_id))
        for track_id, track in tracks.items():
            if not track['db_album_id']:
                tracks[track_id]['release_date'] = album['release_date']
                tracks[track_id]['label'] = album['label']

    print('{} All tracks have album data'.format(PRINT_PREFIX))
    return tracks

def append_artist_genre_data(tracks, batch_size=50):
    """
    Append the genre tags using the Spotify artists API
    See: https://developer.spotify.com/web-api/console/get-several-artists/
    Returns track_list with "genre" appended
    """
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)
    endpoint = "https://api.spotify.com/v1/artists?ids={}"
    # api supports up to 50 ids at a time
    artists = [t['artist_id'] for k,t in tracks.items() if not t['db_artist_id']]
    batches = [artists[i:i + batch_size] for i in range(0, len(artists), batch_size)]

    r_list = []
    for i, batch in enumerate(batches):
        # remove None values from batch
        while True:
            try:
                batch.remove(None)
            except:
                break

        # join ids for api call
        id_str = ','.join(batch)
        r = spotify.request(endpoint.format(id_str))
        r_list += r['artists']
        print('Retrieved artist data {} of {} batches'.format(i + 1, len(batches)))
    artist_data_dict = convert_list_to_dict_by_attribute(r_list, 'id')


    for track_id, track in tracks.items():
        try:
            if not track['db_artist_id']:
                tracks[track_id].setdefault('genres', [])
                tracks[track_id]['genres'] = artist_data_dict[track['artist_id']]['genres']
        except KeyError:
            pass

    print('{} Added all artist data to tracks list'.format(PRINT_PREFIX) )
    return tracks

def get_track_id_from_url(url):
    """
    Return the Spotify track ID from a given url
    Example: https://open.spotify.com/track/r1OmcAT5Y8UPv9qJT4R
    """
    regex = r"open\.spotify\.com\/track\/(\w+)"
    matches = re.search(regex, url)
    assert matches, "No track ID found for {}".format(url)
    return matches.group(1)


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
            print('Connecting to the RDS PostgreSQL database {}...'.format(rds_host))
            self.db = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name)
            print('Successfully connected to AWS RDS PostgreSQL instance.', rds_host)
            self.db.autocommit = True
            self.c = self.db.cursor()
            self.show_db_size()
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
            """, [url])

        except Exception as e:
            raise e
        return True

    # NOTE: cleanup. may not need this
    def is_playlist_followers_processed(self, db_playlist_id, playlist_version, date_str = TODAY):
        """
        Has playlist followers already been processed?
        """

        query = """
            SELECT *
            FROM playlist_processed
            WHERE
                playlist_id = %s
                AND
                playlist_version = %s
                AND
                is_followers_processed = True
                AND
                date_str = %s
        """
        # self.c.execute(query.format(playlist_id, playlist_version, date_str))
        self.c.execute(query, (db_playlist_id, playlist_version, date_str))

        if self.c.fetchone():
            return True
        return False

    def is_playlist_position_processed(self, db_playlist_id, playlist_version):
        """
        Has playlist track position already been processed? (on ANY date)
        """

        query = """
            SELECT *
            FROM playlist_processed
            WHERE
                playlist_id = %s
                AND
                playlist_version = %s
                AND
                is_track_position_processed = True
        """
        self.c.execute(query, (db_playlist_id, playlist_version))

        if self.c.fetchone():
            return True
        return False

    def is_playlist_track_position_attempted(self, playlist_id, playlist_version):
        attempted_query = """
            SELECT *
            FROM playlist_processed
            WHERE
                playlist_id = %s
                AND
                playlist_version = %s
                AND
                is_track_position_processed in (True, False)
        """
        self.c.execute(attempted_query, (playlist_id, playlist_version))

        if self.c.fetchone():
            return True
        return False

    # * in argument list delineates positional arguments from enforced keyword arguments.
    # http://www.informit.com/articles/article.aspx?p=2314818
    def set_playlist_processed(self, playlist_id, playlist_version, date_str, *, is_followers_processed = None, is_track_position_processed = None, is_popularity_processed = None):
        """
        Mark playlist as having followers or all track positions as processed
        """

        query_insert = """
            INSERT INTO playlist_processed
            (playlist_id, playlist_version, date_str)
            VALUES
            (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """

        query_upsert_followers = """
            INSERT INTO playlist_processed
                (playlist_id, playlist_version, date_str, is_followers_processed)
            VALUES
                (%s, %s, %s, %s)
            ON CONFLICT (playlist_id, playlist_version, date_str)
            DO UPDATE SET
                (is_followers_processed) = (%s)
            WHERE
                playlist_processed.playlist_id = %s
                AND
                playlist_processed.playlist_version = '{}'
                AND
                playlist_processed.date_str = %s
        """

        query_upsert_track_position = """
            INSERT INTO playlist_processed
                (playlist_id, playlist_version, date_str, is_track_position_processed)
            VALUES
                (%s, %s, %s, %s)
            --ON CONFLICT (playlist_id, playlist_version, date_str)
            ON CONFLICT (playlist_id, playlist_version, date_str)
            DO UPDATE SET
                (is_track_position_processed) = (%s)
            WHERE
                playlist_processed.playlist_id = %s
                AND
                playlist_processed.playlist_version = '{}'
                AND
                playlist_processed.date_str = %s
        """

        query_upsert_popularity = """
            INSERT INTO playlist_processed
                (playlist_id, playlist_version, date_str, is_popularity_processed)
            VALUES
                (%s, %s, %s, %s)
            --ON CONFLICT (playlist_id, playlist_version, date_str)
            ON CONFLICT (playlist_id, playlist_version, date_str)
            DO UPDATE SET
                (is_popularity_processed) = (%s)
            WHERE
                playlist_processed.playlist_id = %s
                AND
                playlist_processed.playlist_version = '{}'
                AND
                playlist_processed.date_str = %s
        """

        try:
            if is_followers_processed is None and is_track_position_processed is None:
                self.c.execute(
                    query_insert,
                    (playlist_id, playlist_version, date_str)
                )
            elif is_followers_processed and is_track_position_processed is None:
                self.c.execute(
                    query_upsert_followers.format(playlist_version),
                    (playlist_id, playlist_version, date_str, is_followers_processed,
                    is_followers_processed,
                    playlist_id, date_str)
                )
            elif is_track_position_processed and is_followers_processed is None:
                self.c.execute(
                    query_upsert_track_position.format(playlist_version),
                    (playlist_id, playlist_version, date_str, is_track_position_processed,
                    is_track_position_processed,
                    playlist_id, date_str)
                )
            elif is_popularity_processed:
                self.c.execute(
                    query_upsert_popularity.format(playlist_version),
                    (playlist_id, playlist_version, date_str, is_popularity_processed,
                    is_popularity_processed,
                    playlist_id, date_str)
                )
            else:
                # NOTE: if this is a true condition, then do both.
                # I don't expect to use this at all, given how the functions are divided out. This is a fallback.
                # You could create a query that updates both in one UPSERT.
                self.c.execute(
                    query_upsert_followers,
                    (playlist_id, playlist_version, date_str, is_followers_processed,
                    is_followers_processed,
                    playlist_id, date_str)
                )

                self.c.execute(
                    query_upsert_track_position,
                    (playlist_id, playlist_version, date_str, is_track_position_processed,
                    is_track_position_processed,
                    playlist_id, date_str)
                )

        except Exception as e:
            print(e)
            raise

    def update_playlist_processed_version(self, db_playlist_id, date_str, version):
        query = """
            UPDATE playlist_processed
            SET playlist_version = %s
            WHERE
                playlist_id = %s
                AND
                date_str = %s
        """
        self.c.execute(query,
            (version, db_playlist_id, date_str)
        )

    def update_playlist_processed_flag(self, db_playlist_id, date_str, flag_name, flag_value):
        query = """
            UPDATE playlist_processed
            SET {} = %s
            WHERE
                playlist_id = %s
                AND
                date_str = %s
        """
        self.c.execute(query.format(flag_name),
            (flag_value, db_playlist_id, date_str)
        )
        print('updated {} to {} for id {} on {}'.format(flag_name, flag_value, db_playlist_id, date_str))

    # updates version to playlist table
    def update_version(self, playlist_id, latest_version):
        try:
            query = """
                UPDATE playlist
                    SET latest_version = %s
                    WHERE id = %s
            """

            self.c.execute(
                query,
                (latest_version, playlist_id)
            )
        except Exception as e:
            print('update_version error: ', e)
            raise

    def update_processed_version(self, processed_id, version, is_version_updated):
        try:
            query = """
                UPDATE playlist_processed
                SET
                    playlist_version = %s,
                    is_version_updated = %s
                WHERE id = %s
            """

            self.c.execute(
                query,
                (version, is_version_updated, processed_id)
            )

        except Exception as e:
            print('update_version error: ', e)
            raise

    def update_processed_followers(self, processed_id, is_followers_processed):
        try:
            query = """
                UPDATE playlist_processed
                SET
                    is_followers_processed = %s
                WHERE id = %s
            """

            self.c.execute(
                query,
                (is_followers_processed, processed_id)
            )

        except Exception as e:
            print('update_followers error: ', e)
            raise

    def update_processed_positions(self, processed_id, is_track_position_processed):
        try:
            query = """
                UPDATE playlist_processed
                SET
                    is_track_position_processed = %s
                WHERE id = %s
            """

            self.c.execute(
                query,
                (is_track_position_processed, processed_id)
            )
        except Exception as e:
            print('update_positions error: ', e)
            raise

    def get_track_stats(self, service_id, territory_id, track_id):
        """
        Returns a tuple of track stats (track_id, territory_id, service_id, added, last_seen, peak_rank, peak_date)
        """
        query = self.c.execute("""
            SELECT
                first_added,
                last_seen,
                peak_rank,
                peak_date
            FROM peak_track_position
            WHERE
                service_id = %s
            AND
                territory_id = %s
            AND
                track_id = %s
        """, (service_id, territory_id, track_id))
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
        assert a_matches and b_matches, 'Invalid date strings supplied to "order_dates"'
        if a_matches.group(1) < b_matches.group(1):
            return (a, b)
        if a_matches.group(2) < b_matches.group(2):
            return (a, b)
        if a_matches.group(3) < b_matches.group(3):
            return (a, b)
        return (b, a)
    def update_track_stats(self, service_id, territory_id, track_id, position, date_str):
        """
        Update the rolling stats for a track
        """

        position = int(position)
        # latest track stats in the db
        stats = self.get_track_stats(service_id, territory_id, track_id)

        if stats:
            first_added, last_seen, peak_rank, peak_date = stats

        stats_update_query = """
            UPDATE peak_track_position SET
                first_added = %s,
                last_seen = %s,
                peak_rank = %s,
                peak_date = %s
            WHERE
                service_id = %s
            AND
                territory_id = %s
            AND
                track_id = %s
        """

        stats_query = """
            INSERT INTO peak_track_position
            (service_id, territory_id, track_id, first_added, last_seen, peak_rank, peak_date)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        """

        # finds the earlier of the two dates, the current added and the current date query
        # this is important because of the asynchronous nature of collecting the data - the script can download
        # any date from spotify at any time, it is not necessarily in chronological order of when the data is processed
        first_added = self.order_dates(first_added, date_str)[0] if stats else date_str
        # finds the later of the current last_seen and the current date query
        last_seen = self.order_dates(last_seen, date_str)[1] if stats else date_str

        if stats and position < peak_rank:
            # track is ranked higher (has a lower numbered position) when current position is less than old now
            peak_rank = position
            peak_date = date_str
        # peak rank is the same, get the earliest peak date
        elif stats and position == peak_rank:
            peak_date = self.order_dates(peak_date, date_str)[0]
        # position is ranked lower or track doesn't have existing stats
        else:
            peak_rank = peak_rank if stats else position
            peak_date = peak_date if stats else date_str # use the earliest peak date for the peak rank

        # -- Try to update any existing row
        self.c.execute(
            stats_update_query,
            [first_added, last_seen, peak_rank, peak_date, service_id, territory_id, track_id]
        )

        # -- Make sure it exists
        self.c.execute(
            stats_query,
            [service_id, territory_id, track_id, first_added, last_seen, peak_rank, peak_date]
        )

    def add_playlist_tracks(self, date_str, db_playlist_id, playlist_version, track_list):
        """
        input:
            track_list: dict of all songs to add
        Add tracks to the database
        """
        print('Playlist {}: {} tracks to insert'.format(db_playlist_id, len(track_list)))

        for track_id, track in track_list.items():
            db_track_id = track.setdefault('db_track_id', None)
            isrc = track.setdefault('isrc', None)
            position = track.setdefault('position', None)

            try:
                # check if artist or album are in the db
                artist_name = track['artist_name']
                service_album_id = track.get('album_id')
                service_artist_id = track.get('artist_id')
                artist_id = track.get('db_artist_id')
                album_id = track.get('db_album_id')

                # add artist if not in the db
                if not artist_id:
                    # add artist
                    self.c.execute("""
                        INSERT INTO artist
                        (service_id, service_artist_id, artist)
                        VALUES
                        (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    """, (SERVICE_ID, service_artist_id, artist_name))

                    artist_id = self.c.fetchone()[0]
                    print('Artist added {}: {}'.format(artist_id, artist_name))

                # add album if not in the db
                if not album_id:
                    self.c.execute("""
                        INSERT INTO album
                        (service_id, artist_id, service_album_id, album, release_date, label)
                        VALUES
                        (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    """, (SERVICE_ID, artist_id, service_album_id, track['album_name'], track.setdefault('album_release_date', None), track.setdefault('album_label', None) )
                    )
                    album_id = self.c.fetchone()[0]
                    print('Album added: {} {} for {}'.format(album_id, track['album_name'], artist_name))

                # add genres for artist
                for genre in track.setdefault('genres', []):
                    self.c.execute("""
                        INSERT INTO artist_genre
                        (service_id, artist_id, genre)
                        VALUES
                        (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (SERVICE_ID, artist_id, genre))

                # update track table
                #
                if not db_track_id:
                    self.c.execute("""
                        INSERT INTO track
                        (service_id, service_track_id, artist_id, album_id, track, isrc)
                        VALUES
                        (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    """,
                        (SERVICE_ID, track_id, artist_id, album_id, track['track_name'], isrc)
                    )

                    db_track_id = self.c.fetchone()[0]
                    print('Track added {}: {} by {}'.format(db_track_id, track['track_name'], artist_name))

            except Exception as e:
                raise

            # update track_position table
            self.c.execute("""
                INSERT INTO playlist_track_position
                (service_id, playlist_id, playlist_version, track_id, isrc, position, date_str)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (SERVICE_ID, db_playlist_id, playlist_version, db_track_id, isrc, position, date_str)
            )

        return True

    def add_track_popularity(self, db_position_id, popularity):
        print('adding track popularity', db_position_id, popularity)
        try:
            self.c.execute("""
                INSERT INTO playlist_track_popularity
                (position_id, position)
                VALUES
                (%s, %s)
                ON CONFLICT DO NOTHING
            """, (db_position_id, popularity)
            )
            print('added popularity')
        except Error as e:
            print('adding track popularity to db... error: ', e)

    def get_isrc_from_db(self, track_id):
        # RETRIEVE ISRC
        query = """
            SELECT isrc
            FROM track
            WHERE id = %s
        """
        # NOTE: there should be a one-to-one relationship between spotify trackId and db id
        row = db.c.execute(query, [track_id]).fetchone()
        return row[0] if row else False

    def get_territory_id(self, code):
        """
        Retrieve territory_id from region code
        """
        query = self.c.execute("""
            SELECT id FROM territory WHERE code = %s
        """, [code.lower()])

        row = self.c.fetchone()
        return row[0] if row else False

    def get_service_id(self, service_name):
        """
        Retrieve service_id from service name
        """
        query = self.c.execute("""
            SELECT id FROM service WHERE service_name = %s
        """, [service_name])
        row = self.c.fetchone()
        return row[0] if row else False

    def get_artist_id(self, service_id, service_artist_id):
        """
        Retrive service_artist_id
        """
        query = self.c.execute("""
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
        Retrive album db id based on service album id
        INPUT:
            service_id: integer
            service_album_id: text
        """
        query = self.c.execute("""
            SELECT id FROM album
            WHERE
            service_id = %s
            AND
            service_album_id = %s
        """, (service_id, service_album_id)
        )
        row = self.c.fetchone()
        return row[0] if row else False

    def get_owner_id(self, service_id, service_owner_id):
        """
        Retrive db owner_id based on service_owner_id, ie. Spotify's owner id for the playlists
        Or Apple's curator for their playlist
        INPUT:
            service_id: integer
            service_owner_id: text
        """
        query = self.c.execute("""
            SELECT id FROM playlist_owner
            WHERE
            service_id = %s
            AND
            service_owner_id = %s
        """, (service_id, service_owner_id)
        )
        row = self.c.fetchone()
        return row[0] if row else False

    def get_position_id(self, db_playlist_id, version, db_track_id):
        """
        Returns a playlist track position database id, which corresponds to a
        playlist, version, and position for a track.
        """
        service_id = 1 # spotify

        query = self.c.execute("""
            SELECT id FROM playlist_track_position
            WHERE
            service_id = %s
            AND
            playlist_id = %s
            AND playlist_version = %s
            AND track_id = %s
        """, (service_id, db_playlist_id, version, db_track_id)
        )
        row = self.c.fetchone()
        return row[0] if row else None

    def get_playlist_version(self, playlist):
        query = self.c.execute("""
            SELECT latest_version FROM playlist
            WHERE id = %s
        """, [playlist['db_playlist_id']]
        )
        row = self.c.fetchone()
        return row[0] if row else None

    def get_playlist_info(self, service_playlist_id):
        query = self.c.execute("""
            SELECT id, latest_version FROM playlist
            WHERE
            service_id = %s
            AND
            service_playlist_id = %s
        """, (SERVICE_ID, service_playlist_id)
        )
        row = self.c.fetchone()
        return (row[0], row[1]) if row else (False, False)

    def add_playlist_owner(self, service_id, playlist):
        """
        Adds playlist owner to db, returns db owner id
        """
        self.c.execute("""
            INSERT INTO playlist_owner
            (service_id, service_owner_id, alt_name)
            VALUES
            (%s, %s, %s)
            RETURNING id
            """,
            (service_id, playlist['owner_id'], playlist['owner_display_name'])
        )

        db_owner_id = self.c.fetchone()[0]

        return db_owner_id

    def add_playlist_to_db(self, playlist):
        """
        Adds playlist owner to db, returns db owner id
        """
        try:
            if playlist['name'] and playlist['snapshot_id']:
                self.c.execute("""
                    INSERT INTO playlist
                    (service_id, service_playlist_id, name, owner_id, latest_version)
                    VALUES
                    (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (SERVICE_ID, playlist['playlist_id'], playlist['name'], playlist['db_owner_id'], playlist['snapshot_id'])
                )

                db_playlist_id = self.c.fetchone()[0]
                return db_playlist_id
        except psycopg2.IntegrityError as e:
            print('Adding playlist insertion to db failed...')
            return None

    def add_processed_entry(self, db_playlist_id, date_str):
        self.c.execute("""
            INSERT into playlist_processed
            (playlist_id, date_str)
            VALUES
            (%s, %s)
        """, (db_playlist_id, date_str))

    def get_processed_id(self, db_playlist_id, date_str):
        query = """
            SELECT id
            FROM playlist_processed
            WHERE
                playlist_id = %s
                AND
                date_str = %s
        """

        self.c.execute(
            query,
            (db_playlist_id, date_str)
        )

        row = self.c.fetchone()
        return row[0] if row else None

    # updates the db to store latest version of a playlist
    def update_latest_version(self, playlist):
        if playlist.get('snapshot_id') is not None:
            self.c.execute("""
                UPDATE playlist
                SET latest_version = %s
                WHERE id = %s
                """,
                (playlist['snapshot_id'], playlist['db_playlist_id'])
            )

    def flag_playlist_version_change(self, playlist):
        self.c.execute("""
            UPDATE playlist_processed
            SET is_version_updated = %s
            WHERE
                playlist_id = %s
                and
                date_str = %s
        """,
            (not playlist['is_playlist_version_same'], playlist['db_playlist_id'], TODAY)
        )

    # NOTE: could query is_playlist_processed for followers flag to check before selecting value
    def get_followers_for_playlist_from_db(self, playlist, date_str):

        db_playlist_id  = playlist['db_playlist_id']

        query = self.c.execute("""
            SELECT id, followers, date_str
            FROM playlist_followers
            WHERE
            playlist_id  = %s
            AND
            date_str = %s
            """,
            (db_playlist_id, date_str)
        )

        row = self.c.fetchone()

        return True if row else False

    def get_playlists_with_minimum_follower_count_from_db(self, followers = 1000, date_of_followers = '2018-02-02'):
        query = """
            SELECT
                playlist.id,
                playlist.service_playlist_id,
                po.service_owner_id,
                playlist.latest_version,
                pf.followers,
                pf.date_str
            FROM playlist
            INNER JOIN playlist_followers pf
                ON pf.playlist_id = playlist.id
            INNER JOIN playlist_owner po
                ON po.id = playlist.owner_id
            WHERE pf.date_str = %s
            AND pf.followers >= %s
            ORDER BY followers desc
        """

        self.c.execute(
            query,
            (date_of_followers, followers)
        )

        playlists_list = []
        for row in self.c.fetchall():
            playlist = {}
            # fetch db columns which we will use later in the processing of the playlist to get tracks, etc.
            playlist['db_playlist_id'] = row[0]
            playlist['spotify_playlist_id'] = row[1]
            playlist['owner_id'] = row[2] # this is the spotify owner id, ie.  a string 'filtr', or 'spotify' NOT the db id
            playlist['db_playlist_version'] = row[3] # construct this playlist object as if it were coming from the Spotify API with keys like 'snapshot_id'
            playlists_list.append(playlist)

        return playlists_list

    def get_playlists_position_processed_by_date(self, date_str = TODAY):
        query = """
            SELECT playlist_id from playlist_processed
            WHERE
                date_str = %s
                and
                is_track_position_processed = True
        """

        self.c.execute(
            query,
            [date_str]
        )

        playlist_ids = []
        for row in self.c.fetchall():
            playlist_ids.append(row[0])

        return playlist_ids

    def get_playlists_track_not_processed_by_date(self, date_str = TODAY):
        # is_track_position can be true or false if it processed correctly or failed.
        # We check for NOT NULL so that any unprocessed playlists can go, without getting stuck on certain playlists if they fail repeatedly.
        query = """
            SELECT playlist_id from playlist_processed
            WHERE
                date_str = %s
                and
                is_track_position_processed IS NULL
        """

        self.c.execute(
            query,
            [date_str]
        )

        playlist_ids = []
        for row in self.c.fetchall():
            playlist_ids.append(row[0])

        return playlist_ids

    def get_playlists_with_version_update_by_date(self, date_str = TODAY):
        query = """
            SELECT playlist_id from playlist_processed
            WHERE
                date_str = %s
                and
                is_version_updated = True
        """

        self.c.execute(
            query,
            [date_str]
        )

        playlist_ids = []
        for row in self.c.fetchall():
            playlist_ids.append(row[0])

        return playlist_ids

    def get_playlists_followers_processed_by_date(self, date_str = TODAY):
        query = """
            SELECT playlist_id from playlist_processed
            WHERE
                date_str = %s
                and
                is_followers_processed = True
        """

        self.c.execute(
            query,
            [date_str]
        )

        playlist_ids = []
        for row in self.c.fetchall():
            playlist_ids.append(row[0])

        return playlist_ids

    def get_playlists_versions_processed_by_date(self, date_str = TODAY):
        query = """
            SELECT playlist_id from playlist_processed
            WHERE
                date_str = %s
                and
                is_version_updated IS NOT NULL
        """

        self.c.execute(
            query,
            [date_str]
        )

        playlist_ids = []
        for row in self.c.fetchall():
            playlist_ids.append(row[0])

        return playlist_ids

    def add_playlist_followers(self, service_id, date_str, db_playlist_id, version, followers):
        try:
            self.c.execute("""
                INSERT INTO playlist_followers
                (service_id, playlist_id, playlist_version, followers, date_str)
                VALUES
                (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (service_id, db_playlist_id, version, followers, date_str))

            print("{} playlist id's followers added to db".format(db_playlist_id))
            return True
        except psycopg2.IntegrityError as e:
            if e.pgcode != '23502': # Not Null constraint: https://www.postgresql.org/docs/8.1/static/errcodes-appendix.html
                print('Postgresql Integrity Error: ', e)
            print('Adding playlist followers to db failed...')
            return False

    # get all info from db from most recent date of latest playlist version
    #
    def get_db_tracks_by_playlist(self, service_playlist_id, playlist_version):
        id_query = self.c.execute("""
                SELECT id from playlist
                WHERE service_playlist_id = %s
        """, [service_playlist_id])

        playlist_id = self.c.fetchone()[0]

        query = self.c.execute("""
            SELECT
                ptp.track_id,
                ptp.isrc,
                ptp.position,
                t.service_track_id
            FROM playlist_track_position ptp
			INNER JOIN playlist p
				ON p.id = ptp.playlist_id
            INNER JOIN track t
                ON t.id = ptp.track_id
            WHERE ptp.playlist_id = %s
            AND ptp.playlist_version = %s
            AND ptp.date_str = (
                SELECT max(date_str) FROM playlist_track_position
                WHERE playlist_id = %s
                AND playlist_version = %s
            )
        """, (playlist_id, playlist_version, playlist_id, playlist_version)
        )

        # construct tracks playlist with attributes needed for playlist_track_position
        track_list = []

        for row in self.c.fetchall():
            track = {}
            track['db_track_id'] = row[0]
            track['isrc'] = row[1]
            track['position'] = row[2]
            track['track_id'] = row[3]
            track_list.append(track)

        return track_list

def get_playlists_for_user(user_id, nextUrl=None):

    if not nextUrl:
        endpoint = "{}/users/{}/playlists?limit={}".format(SPOTIFY_API, user_id, MAX_LIMIT_QUERY)
    else:
        endpoint = nextUrl

    try:
        r = spotify.request(endpoint)

        playlists = []

        for p in r['items']:
            playlist = {}
            playlist['playlist_id'] = p['id']
            playlist['owner_id'] = p['owner']['id']
            playlist['owner_display_name'] = p['owner'].get('display_name', '')
            playlist['name'] = p['name']
            playlist['snapshot_id'] = p['snapshot_id']
            playlist['num_tracks'] = p['tracks']['total']
            playlist['href'] = p['href']
            playlist['uri'] = p['uri']
            playlists.append(playlist)

        # if there are more values to return, request more and add to the list
        if not r['next']:
            print('-----{} playlists-----'.format(user_id))
            return playlists
        else:
            print('...getting more playlists for {}'.format(user_id))
            return playlists + get_playlists_for_user(user_id, r['next'])

        return playlists

    except Exception as e:
        logger.warn(e)

def get_all_playlists_for_users(users):
    all_playlists = []
    for user in users:
        user_playlists = get_playlists_for_user(user)
        all_playlists += user_playlists
        print('{} playlists:'.format(len(user_playlists)))
        print_divider(len(user_playlists), '*')
    print_divider(40)
    print('TOTAL PLAYLISTS FOR ALL USERS {}'.format(len(all_playlists)))
    return all_playlists

# Print Helper
#
def print_divider(number, divider='-'):
    print(divider * number)

# processes track positions for playlists with a version update
def get_daily_unprocessed_playlists_positions(limit = 150):
    # gets a skeleton playlist "object" from the db
    # NOTE: can also create a new function that createes the skeleton object given a list of playlist ids...
    playlists = db.get_playlists_with_minimum_follower_count_from_db()

    # which to process?
    db_ids_with_updated_version = db.get_playlists_with_version_update_by_date() # ones with updated versions
    db_ids_processed = db.get_playlists_position_processed_by_date() # ones that are already processed
    db_ids_unprocessed = list(set(db_ids_with_updated_version) - set(db_ids_processed))

    unprocessed_playlists = [pl for pl in playlists if pl['db_playlist_id'] in db_ids_unprocessed]

    return unprocessed_playlists[0:limit]

# Strategy of daily unprocessed lists
#
def get_daily_unprocessed_playlists_followers(limit = 150):
    # gets a skeleton playlist "object" from the db
    playlists = db.get_playlists_with_minimum_follower_count_from_db()
    db_ids_processed = db.get_playlists_followers_processed_by_date()
    unprocessed_playlists = [pl for pl in playlists if pl['db_playlist_id'] not in db_ids_processed]
    return unprocessed_playlists[0:limit]

def get_daily_unprocessed_versions(limit = 150):
    playlists = db.get_playlists_with_minimum_follower_count_from_db()
    db_ids_processed = db.get_playlists_versions_processed_by_date()
    unprocessed_playlists = [pl for pl in playlists if pl['db_playlist_id'] not in db_ids_processed]
    return unprocessed_playlists[0:limit]

# takes a list of playlist objects and removes objects where the spotify db id is in the unwanted list
def remove_unwanted_playlists(playlists):
    return [pl for pl in playlists if pl['spotify_playlist_id'] not in UNWANTED_PLAYLIST_SPOTIFY_IDS]

# Add follower count for playlists
# Add follower count to db for each playlist
# Update latest snapshot version!
# INPUT: playlists object
#
def append_playlist_followers_and_update_version(playlists):
    for playlist_id, playlist in playlists.items():
        # NOTE: can change to check
        # if not db.get_followers_for_playlist_from_db(playlist, TODAY):
        try:
            if not db.is_playlist_followers_processed(playlist['db_playlist_id'], playlist['db_playlist_version'], TODAY):
                # get data
                owner_id = playlist.get('owner_id')

                # if getting followers from api fails, it returns None, NoneType
                # which will result in Null values in the db
                followers, snapshot_id = get_followers_for_playlist_from_api(owner_id, playlist_id)
                playlists[playlist_id]['followers'] = followers
                playlists[playlist_id]['snapshot_id'] = snapshot_id

                # write to db
                is_followers_added = db.add_playlist_followers(SERVICE_ID, TODAY, playlist)
                # db.update_latest_version(playlist)

                # mark playlist as followers having been processed
                if is_followers_added:
                    db.update_playlist_processed_flag(playlist['db_playlist_id'], TODAY, 'is_followers_processed', True)
                    # db.set_playlist_processed(playlist['db_playlist_id'], snapshot_id, TODAY, is_followers_processed = True)
                else:
                    db.update_playlist_processed_flag(playlist['db_playlist_id'], TODAY, 'is_followers_processed', True)
                    # db.set_playlist_processed(playlist['db_playlist_id'], snapshot_id, TODAY, is_followers_processed = False)


                # UPDATE PLAYLIST VERSION
                # UPDATE PLAYLIST PROCESSED WITH VERSION
                # UPDATE PLAYLIST PROCESSED WITH IS VERSION UPDATED FLAG
                if playlist['db_playlist_version'] != snapshot_id:
                    db.update_latest_version(playlist)
                    db.update_playlist_processed_version(playlist['db_playlist_id'], TODAY, snapshot_id)
                    db.update_playlist_processed_flag(playlist['db_playlist_id'], TODAY, 'is_version_updated', True)
                elif playlist['db_playlist_version'] == snapshot_id:
                    db.update_playlist_processed_flag(playlist['db_playlist_id'], TODAY, 'is_version_updated', False)
                else:
                    continue

                #  DOES THIS HAVE PLAYLIST VERSION SAME KEY??>
                # db.flag_playlist_version_change(playlist)
            else:
                print('{} db followers already in db for {}'.format(playlist_id, TODAY))
        except psycopg2.IntegrityError as e:
            print('Adding playlist followers to db failed...')
            continue
    return playlists

def get_followers_for_playlist_from_api(user_id, playlist_id):
    query_params = 'fields=followers.total,snapshot_id'
    endpoint = '{}/users/{}/playlists/{}?{}'.format(SPOTIFY_API, user_id, playlist_id, query_params)

    # r will return the json data or False (if error in the request)
    r = spotify.request(endpoint)

    if r and r['followers']:
        followers = r['followers']['total']
        snapshot_id = r['snapshot_id']
        return (followers, snapshot_id)
    else:
        print("Couldn't get followers or snapshot id for user {} and playlist {}... request returned False".format(user_id, playlist_id))

    return (None, None)

def get_version_from_api(user_id, playlist_id):
    query_params = 'fields=followers.total,snapshot_id'
    endpoint = '{}/users/{}/playlists/{}?{}'.format(SPOTIFY_API, user_id, playlist_id, query_params)

    # r will return the json data or False (if error in the request)
    r = spotify.request(endpoint)

    if r:
        snapshot_id = r['snapshot_id']
        return snapshot_id
    else:
        print("Couldn't get snapshot id for user {} and playlist {}... request returned False".format(user_id, playlist_id))
    return None

def fetch_playlist_tracks_from_api(user_id, playlist_id, tracks=[], nextUrl=None):
    """
    Return value example:
    [
        {
            'artist_id': '21E3waRsmPlU7jZsS13rcj',
            'artist_name': 'Ne-Yo',
            'album_id': '1nv3KEXZPmcwOXMoLTs1vn',
            'album_name': '1nv3KEXZPmcwOXMoLTs1vn',
            'isrc': 'USUM70826981',
            'track_id': '6glklpxk7EtKIdxA3kYQS5',
            'track_name': 'Miss Independent',
            'track_uri': 'spotify:track:6glklpxk7EtKIdxA3kYQS5',
            'popularity': 69,
            'position': 3
        }, {}, {}, ...
    ]
    """
    max_query = 100
    if not nextUrl:
        endpoint = "{}/users/{}/playlists/{}/tracks?limit={}".format(SPOTIFY_API, user_id, playlist_id, max_query)
    else:
        endpoint = nextUrl

    try:
        r = spotify.request(endpoint)

        # if there is no response, return an empty track_list
        if not r:
            return []
        else:
            tracks = tracks + r.get('items')

        # finished receiving all tracks for playlist
        if not r.get('next'):
            tracks_list = []
            for idx, t in enumerate(tracks):
                try:
                    track = {}
                    track['position'] = idx + 1
                    try:
                        track['artist_id'] = t['track']['album']['artists'][0]['id']
                    except (IndexError, TypeError) as e:
                        logger.warn('Track does not have artist id attached', tracks)
                        track['artist_id'] = None

                    try:
                        track['artist_name'] = t['track']['album']['artists'][0]['name'] # NOTE: take only first artist, as is primary artist, not all collaborators
                    except (IndexError, TypeError) as e:
                        logger.warn('Track does not have id or artist name attached', tracks)
                        track['artist_name'] = ''

                    track['isrc'] = t['track']['external_ids'].get('isrc')
                    track['track_id'] = t['track']['id']
                    track['track_name'] = t['track']['name']
                    track['track_uri'] = t['track']['uri']
                    track['popularity'] = t['track']['popularity']

                    track['album_id'] = t['track']['album'].get('id')
                    track['album_name'] = t['track']['album'].get('name')
                    tracks_list.append(track)
                except TypeError as e:
                    logger.warn(e)
                    continue
                except Exception as e:
                    logger.warn(e)
                    continue
            print('{}:{} got all {} tracks'.format(user_id, playlist_id, len(tracks_list)))
            return tracks_list

        # there are more tracks in the playlist, call function recursively
        else:
            return fetch_playlist_tracks_from_api(user_id, playlist_id, tracks, r.get('next'))

    except TypeError as e:
        logger.warn(e)

# returns db owner id to a playlist Object, else creates a new owner and returns the id
#
def get_db_owner_id(service_id, playlist):
    try:

        db_owner_id = db.get_owner_id(service_id, playlist['owner_id']) # spotify service_id = 1, placeholder

        if db_owner_id:
            return int(db_owner_id)
        else:
            return db.add_playlist_owner(service_id, playlist)

    except Exception as e:
        print(e)
        raise
        return None

def append_db_owner_id(playlists):
    for playlist_id, playlist in playlists.items():
        try:
            # spotify service_id = 1, placeholder
            playlist.setdefault('db_owner_id', None)
            playlist['db_owner_id'] = get_db_owner_id(SERVICE_ID, playlist)

        except Exception:
            raise
    return playlists

# if playlist tracks for the playlist version are not in the db, add them


def append_data_and_add_single_playlist_positions(playlist):
    try:
        tracks_list = []
        tracks_dict = {}

        # get tracks for the playlist from the API
        tracks_list = fetch_playlist_tracks_from_api(playlist['owner_id'], playlist['spotify_playlist_id'])

        if len(tracks_list) > 0:
            tracks_dict = convert_list_to_dict_by_attribute(tracks_list, 'track_id')
            tracks_dict = append_track_artist_album_db_ids(tracks_dict)
            tracks_dict = append_artist_genre_data(tracks_dict)
            tracks_dict = append_album_label_release_data(tracks_dict)

            # add the tracks list to the playlist object
            playlist['tracks'] = tracks_dict

            # NOTE: we are assuming the 'db_playlist_version' == 'version_from_api', we haven't used get_version_from_api
            # to fetch and compare. This is based on proximity of time between when the
            # version is updated in the db and when we are fetching new playlists... should be very soon after, AND
            # this shouldmake this check redundant. There could be a small discrepancy, however, if the version changes
            # and we are not double-checking the version.
            #
            db.add_playlist_tracks(TODAY, playlist['db_playlist_id'], playlist['db_playlist_version'], tracks_dict)
            print('{} All tracks added for playlist id {}'.format(PRINT_PREFIX, playlist['db_playlist_id']))
            return True
        else:
            return False

    except TypeError:
        return False

def append_all_playlist_tracks(playlists):
    for playlist_id, playlist in playlists.items():
        append_data_and_add_single_playlist_positions(playlist)

def append_playlist_tracks(playlists):
    for playlist_id, playlist in playlists.items():
        try:
            # if daily track position is not processed
            if not db.is_playlist_position_processed(playlist['db_playlist_id'], playlist['snapshot_id']):
                playlists[playlist_id].setdefault('tracks', {})
                playlists[playlist_id].setdefault('is_track_list_from_db', False) # flag for how to insert tracks into db

                # if latest version is same as db order, grab tracks from db to save API call
                tracks_list = []
                tracks_dict = {}

                # Get the list of tracks from the DB, if the playlist version is the same, rather than from the api
                # NOTE: we will not be writing to the DB when the playlist version is the same. Rather, the tracks for a
                # playlist version will be queried on report runtime generation
                #
                if playlist.get('is_playlist_version_same'):
                    playlists[playlist_id]['is_track_list_from_db'] = True
                    tracks_list = db.get_db_tracks_by_playlist(playlist_id, playlist['snapshot_id'])
                    tracks_dict = convert_list_to_dict_by_attribute(tracks_list, 'track_id') # track_id is spotify track id (not db track id)

                    print('Querying db for tracks...')

                # if something is missing from the database
                # or if playlist version is different, append new data from new tracks
                if len(tracks_dict) != playlist.get('num_tracks'):
                    print('Querying Spotify API for tracks...')
                    playlists[playlist_id]['is_track_list_from_db'] = False # reset flag

                    # construct the tracks list
                    tracks_list = fetch_playlist_tracks_from_api(playlist['owner_id'], playlist['playlist_id'])
                    tracks_dict = convert_list_to_dict_by_attribute(tracks_list, 'track_id')
                    # tracks_dict = playlists[playlist_id].setdefault('tracks', {})
                    tracks_dict = append_track_artist_album_db_ids(tracks_dict)
                    tracks_dict = append_artist_genre_data(tracks_dict)
                    tracks_dict = append_album_label_release_data(tracks_dict)
                    print('Appended new track data...')

                # add the tracks list to the playlist object
                playlists[playlist_id]['tracks'] = tracks_dict

                # write to database, batman
                db.add_playlist_tracks(TODAY, playlist['db_playlist_id'], playlist['snapshot_id'], tracks_dict)

                # mark it as Processed, hulkster
                db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = True)

            print('{} All tracks added for playlist id {}'.format(PRINT_PREFIX, playlist['db_playlist_id']))
            print('*' * 40)

        except Exception as e:
            print(e)
            logger.warning('Warning %s for playlist id %s', e, playlist_id)
            continue
    return playlists

#
# converts a list with a attribute (usually an id value, such as spotify's track id)
# to a dictionary with the attribute as the key of the new dictionary, and the list object as the value
# example:
# INPUT:
#   list = [{'id': 123, 'data': xyz}, {'id': 234, 'data': abc}]
#   attribute = 'id'
# OUTPUT:
# {
#   123: {'id': 123, 'data': xyz},
#   234: {'id': 234, 'data': abc}
# }
#
def convert_list_to_dict_by_attribute(item_list, attribute):
    converted = {}
    for item in item_list:
        key = item[attribute]
        converted[key] = item
    return converted

# itertools recipe: List unique elements, preserving order. Remember all elements ever seen.
#
# https://docs.python.org/2/library/itertools.html#recipes
# https://stackoverflow.com/questions/15511903/remove-duplicates-from-a-list-of-dictionaries-when-only-one-of-the-key-values-is
#
def unique_everseen(iterable, key=None):
    # "List unique elements, preserving order. Remember all elements ever seen."
    # unique_everseen('AAAABBBCCDAABBB') --> A B C D
    # unique_everseen('ABBCcAD', str.lower) --> A B C D
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in ifilterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element


# NOTE: batch update by users once a week or something. Not Used YET!!!
def refresh_playlists_of_users(users):
    # INITIALIZE VARIABLES
    service_name = 'Spotify'

    # NOTE: DO NOT GET ALL PLAYLISTS EVERY TIME
    playlists_list = get_all_playlists_for_users(users)
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'spotify_playlist_id')
    playlists = append_db_owner_id(playlists)

    print('writing to db')
    append_playlist_followers_and_update_version(playlists)

def process_daily_followers(playlists_list):
    service_name = 'Spotify'
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'spotify_playlist_id')
    playlists = append_playlist_followers_and_update_version(playlists)

def append_followers_from_api(playlist):
    playlist['followers'], playlist['version_from_api'] = get_followers_for_playlist_from_api(playlist['owner_id'], playlist['spotify_playlist_id'] )
    return playlist

def is_followers_appended(playlist):
    if playlist['followers'] is None or playlist['version_from_api'] is None:
        return False
    else:
        return True

# appends latest version (ie. snapshot_id from spotify api)
def append_version_from_api(playlist):
    # {
    #   "db_playlist_id": 2795,
    #   "spotify_playlist_id": "37i9dQZF1DXcBWIGoYBM5M",
    #   "owner_id": "spotify",
    #   "db_playlist_version": "3dopR5i84J0KVjWLZKmfp6stkmi7JIA6VjLAUQxs/taqZHF1ocK9a7rcnahcMtC1qSocs3DGvHk="
    # }
    # as specified by def get_playlists_with_minimum_follower_count_from_db()

    followers, version = get_followers_for_playlist_from_api(playlist['owner_id'], playlist['spotify_playlist_id'])
    playlist['version_from_api'] = version
    # playlist['version_from_api'] = get_version_from_api(playlist['owner_id'], playlist['spotify_playlist_id'])

    return playlist

# Output: adds a version comparison boolean as a key-value pair to each playlist "object"
def append_version_updated_flag(playlist):

    if playlist['version_from_api'] == playlist['db_playlist_version']:
        playlist['is_version_updated'] = False
    # ensure version from api is not null
    elif (playlist['version_from_api'] is None) or (playlist['db_playlist_version'] is None):
        playlist['is_version_updated'] = None
    else:
        playlist['is_version_updated'] = True

    return playlist

# regular everyday processing of playlist tracks, once all playlists have their tracks processed the first time
# if process_all is True, don't check whether the playlist version is the latest, process all playlists
#
def process_daily_track_position(playlists_list):
    # playlists_list = db.get_playlists_with_minimum_follower_count_from_db(followers = 1000)
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'spotify_playlist_id')

    for playlist_id, playlist in playlists.items():

        # update playlist version to latest snapshot id, if necessary
        # NOTE: can rewrite the unprocessed playlists query to grab the playlists with CHANGED playlist versions ONLY
        # playlist['snapshot_id'] is the version of the latest poll of playlist versions and followers (minimum follower count function)
        # db.get_playlist_version is the latest version of the playlist stored in the db, which would be updated to the latest version when the followers
        # count is updated daily.
        #

            try:
                append_data_and_add_single_playlist_positions(playlist)
                db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = True)
            except TypeError:
                db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = False)
                # if URLerror returns false, and NoneType is not iterable
                continue

    return 'processed daily track positions for given playlists'

# processes all playlist track positions, does not check for playlist version before processing
#
def fetch_all_playlist_positions(playlists_list):
    # playlists_list = db.get_playlists_with_minimum_follower_count_from_db(followers = 1000)
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'spotify_playlist_id')

    for playlist_id, playlist in playlists.items():
        try:
            did_succeed = None
            if not db.is_playlist_position_processed(playlist['db_playlist_id'], playlist['snapshot_id']):
                did_succeed = append_data_and_add_single_playlist_positions(playlist)

            if did_succeed:
                print('PLAYLIST POSITIONS PROCESSED', playlist['db_playlist_id'], playlist['snapshot_id'])
                db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = True)
            else:
                db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = False)
        except TypeError:
            db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = False)
            # if URLerror returns false, and NoneType is not iterable
            continue

    return 'periodic processing complete - processed daily track positions for all playlists'

# for each track in a playlist, this fetches the changing popularities for those tracks
def fetch_playlist_popularities(playlists_list):
    # NOTE: possible data discrepancy can creep in here, where the version/snapshot id changes in between
    # the time when scraping followers or track positions AND THEN popularity. This will create a small window when the version
    # is different and there are no positions available. This function does not do anything in that case...
    # orphaning the playlist and popularities.

    playlists = convert_list_to_dict_by_attribute(playlists_list, 'spotify_playlist_id')
    for spotify_playlist_id, playlist in playlists.items():
        db_playlist_id = playlist['db_playlist_id']
        version = playlist['snapshot_id']

        # Check that positions have been processed BECAUSE We are keying off the playlist_track_position ID primary key column
        if db.is_playlist_position_processed(db_playlist_id, version):
            print('processed', db_playlist_id, version)
            # What follows is a lite workfrom SIMILAR TO append_data_and_add_single_playlist_positions(playlist)
            tracks_list = fetch_playlist_tracks_from_api(playlist['owner_id'], spotify_playlist_id)

            if len(tracks_list) > 0:
                tracks_dict = convert_list_to_dict_by_attribute(tracks_list, 'track_id') # KEYED off the spotify track id
                tracks_dict = append_track_artist_album_db_ids(tracks_dict)

            print('tracks dict is', tracks_dict)

            for spotify_track_id, track in tracks_dict.items():
                db_position_id = db.get_position_id(db_playlist_id, version, spotify_track_id)
                print('db_position_id is', db_position_id)

                # NOTE: if you want to be doubly safe, you can compare spotify track['track_id'] with track['position'] in the db
                 # to ensure they match. However, we'll play it a little fast and loose here and assume it's correct.
                db.add_track_popularity(db_position_id, track['popularity'])
        else:
            print('not processed', db_playlist_id, version)

        print('setting processed')
        db.set_playlist_processed(db_playlist_id, version, TODAY, is_popularity_processed = True)

    return 'popularity fetching complete'

### AWS LAMBDA HANDLERS
#
#
#
#
#

def refresh_playlists_of_users_handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # set default users to get playlists from
    users = ALL_USERS

    # if input, overwrite with array of spotify owners (as strings)
    if event.get('users'):
        users = event.get('users')

    refresh_playlists_of_users(users)

    # clean up
    db.close_database()
    print('closed database connection')
    return 'finished spotify updating of all playlists'

def create_playlist_processed_entry_handler(event, context):
    # write a new daily entry for the playlist
    # This processed row is just an empty container with playlist id and date
    #
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # event is a list (array) of playlist objects from the db
    playlists = db.get_playlists_with_minimum_follower_count_from_db()

    entries = 0
    for playlist in playlists:
        db.add_processed_entry(playlist['db_playlist_id'], TODAY)
        entries += 1

    # clean up
    db.close_database()
    return 'Created {} playlist processed entries for {}'.format(entries, TODAY)

def fetch_playlist_versions_handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # event is a list (array) of playlist objects from the db
    playlists = event

    completed_count = 0
    for playlist in playlists:
        playlist = append_version_from_api(playlist)
        playlist = append_version_updated_flag(playlist)

        # update playlist_processed table with version and version updated flag
        # if None, there was an error in getting version from API or it's NULL in the db
        #
        if playlist['is_version_updated'] != None:
            completed_count += 1
            processed_id = db.get_processed_id(playlist['db_playlist_id'], TODAY)
            db.update_processed_version(processed_id, playlist['version_from_api'], playlist['is_version_updated'])

        # update playlist version
        if playlist['is_version_updated'] == True:
            db.update_version(playlist['db_playlist_id'], playlist['version_from_api'])

    # clean up
    db.close_database()
    print('closed database connection')
    return 'completed version updates for {} of {} playlists.'.format(completed_count, len(playlists))

def fetch_playlist_followers_handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # Setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # Input is a list of playlist objects from the db
    playlists_list = event

    for playlist in playlists_list:
        # NOTE: We append version, but don't do anything with it
        # versions are updated previous to this in another lambda, the assumption is
        # that the version in the db is current. If you want to do another check, you can do it here
        #
        playlist = append_followers_from_api(playlist)

        # If you want to be 100% clear, check 'version_from_api' = 'db_playlist_version'

        # Add followers to db table
        processed_id = db.get_processed_id(playlist['db_playlist_id'], TODAY)


        # if followers are valid from api, and added to db successfully...
        if is_followers_appended(playlist) and db.add_playlist_followers(SERVICE_ID, TODAY, playlist['db_playlist_id'], playlist['db_playlist_version'], playlist['followers']):
            db.update_processed_followers(processed_id, True)
        else:
            db.update_processed_followers(processed_id, False)

        # NOTE: cleanup: set_playlist_processed() is probably not used.
        # NOTE: cleanup: append_playlist_followers_and_update_version() is probably not used
        # NOTE: cleanup: process_daily_followers() is probably not used.

    # clean up
    db.close_database()
    return 'finished playlist followers'

def fetch_playlist_positions_handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # input is a list of playlist objects from the db
    playlists_list = event
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'spotify_playlist_id')

    processed_total = 0

    # process_daily_track_position(playlists_list)
    for playlist_id, playlist in playlists.items():
        # fetch and write positions to db, if it succeeds...
        processed_id = db.get_processed_id(playlist['db_playlist_id'], TODAY)
        if append_data_and_add_single_playlist_positions(playlist):
            db.update_processed_positions(processed_id, True)
            processed_total += 1
        else:
            db.update_processed_positions(processed_id, False)

    db.close_database()
    return 'completed {}/{} playlist positions successfully'.format(processed_total, len(playlists_list))

def get_playlists_with_minimum_follower_count_from_db_handler(event, context):
    global db
    db = TrackDatabase()

    playlists = db.get_playlists_with_minimum_follower_count_from_db()

    db.close_database()

    return playlists

def set_processed_playlist_position_handler(event, context):
    # set all the ones that have not changed the version
    return

def get_daily_unprocessed_playlists_followers_handler(event, context):
    global db
    db = TrackDatabase()

    # NOTE: TODO POTENTIAL create lambda function to dynamically change the limit. This will also entail creating
    # a function to dynamically evenly break up the list into 3 even sized chunks, and for the parallel functions
    # to grab the value off the input from the lambda... It may be too much trouble, and may not change as much as you think!
    # If you do implement it, it will be simple like this:
    # limit = event.playlist_limit


    unprocessed_playlists = get_daily_unprocessed_playlists_followers()
    unprocessed_playlists = remove_unwanted_playlists(unprocessed_playlists)

    db.close_database()

    return unprocessed_playlists

def get_daily_unprocessed_playlists_followers_length_handler(event, context):
    global db
    db = TrackDatabase()

    # NOTE: TODO POTENTIAL create lambda function to dynamically change the limit. This will also entail creating
    # a function to dynamically evenly break up the list into 3 even sized chunks, and for the parallel functions
    # to grab the value off the input from the lambda... It may be too much trouble, and may not change as much as you think!
    # If you do implement it, it will be simple like this:
    # limit = event.playlist_limit


    unprocessed_playlists = get_daily_unprocessed_playlists_followers(limit = 10000)
    unprocessed_playlists = remove_unwanted_playlists(unprocessed_playlists)

    db.close_database()

    return len(unprocessed_playlists)

def get_daily_unprocessed_playlists_versions_handler(event, context):
    global db
    db = TrackDatabase()

    unprocessed_playlists = get_daily_unprocessed_versions()
    unprocessed_playlists = remove_unwanted_playlists(unprocessed_playlists)

    db.close_database()

    return unprocessed_playlists

def get_daily_unprocessed_playlists_versions_length_handler(event, context):
    global db
    db = TrackDatabase()

    unprocessed_playlists = get_daily_unprocessed_versions(limit = 10000)
    unprocessed_playlists = remove_unwanted_playlists(unprocessed_playlists)

    db.close_database()

    return len(unprocessed_playlists)

def get_daily_unprocessed_playlists_positions_handler(event, context):
    global db
    db = TrackDatabase()
    unprocessed_playlists = get_daily_unprocessed_playlists_positions()
    unprocessed_playlists = remove_unwanted_playlists(unprocessed_playlists)
    db.close_database()
    return unprocessed_playlists

def get_daily_unprocessed_playlists_positions_length_handler(event, context):
    global db
    db = TrackDatabase()
    unprocessed_playlists = get_daily_unprocessed_playlists_positions(limit = 10000)
    unprocessed_playlists = remove_unwanted_playlists(unprocessed_playlists)
    db.close_database()
    return len(unprocessed_playlists)

def fetch_all_playlist_positions_handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # input is a list of playlist objects from the db
    playlists_list = event

    fetch_all_playlist_positions(playlists_list)

    db.close_database()
    print('closed database connection')
    return 'finished playlist track positions'

def fetch_playlist_popularities_handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # input is a list of playlist objects from the db
    playlists_list = event

    fetch_playlist_popularities(playlists_list)

    db.close_database()
    print('closed database connection')
    return 'finished playlist track popularity'
