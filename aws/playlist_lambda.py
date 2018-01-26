import sys
sys.path.insert(0, './aws_packages') # local relative path of aws lambda packages for zipping

import csv, codecs, re, json, os, base64, time, hashlib, ssl, datetime, logging, errno
from pprint import pprint as pprint
from datetime import datetime, date, timedelta
from socket import error as SocketError
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
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
LANGUAGE_PLAYLIST_DB_IDS = [1153, 1162, 1163, 1164, 1165, 1166, 1167, 1168, 1169, 1170, 960, 1006, 978, 961, 962, 984] # NOTE: added in from 960 onwards manually, since function was getting stuck
LANGUAGE_PLAYLIST_SPOTIFY_IDS = ['37i9dQZF1DXc6li3e9oatQ', '37i9dQZF1DX1QCg8MO15wF', '37i9dQZF1DWTJSgpZmw7H2', '37i9dQZF1DWVrSKB2Pc3PY', '37i9dQZF1DWW6K9D6JN1rY', '37i9dQZF1DX0yHwYvqyUJQ', '37i9dQZF1DX2SgxzTVd6bU', '37i9dQZF1DX60lVXkfYly8', '37i9dQZF1DWSsCx004HXRd', '37i9dQZF1DWSIZLZz4Kogf']


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
        r = urlopen(url, context=SSL_CONTEXT)
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
            print('URLError... ', str(e.reason), ' for ', url)
            return False
        except ConnectionResetError as e:
            print('Error 54: Connection reset error. ', str(e))
            return get_page(url, cache, count, time.time())
        except HTTPError as err:
            if err.code == 400:
                print('HTTP400 error... {}'.format(url))
                return False
            elif err.code == 401:
                print('HTTP401 error... {}'.format(url))
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

def append_db_ids(tracks):
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

def append_album_data(tracks, batch_size=20):
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

def append_artist_data(tracks, batch_size=50):
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

    def is_playlist_followers_processed(self, playlist_id, playlist_version):
        """
        Has playlist followers already been processed?
        """

        query = """
            SELECT *
            FROM playlist_processed
            WHERE
                playlist_id = {}
                AND
                playlist_version = '{}'
                AND
                is_followers_processed = True
        """
        print('is_playlist_followers_processed:', playlist_id, playlist_version)
        self.c.execute(query.format(playlist_id, playlist_version))

        if self.c.fetchone():
            return True
        return False

    def is_playlist_track_position_processed(self, playlist_id, playlist_version):
        """
        Has playlist track position already been processed?
        """

        query = """
            SELECT *
            FROM playlist_processed
            WHERE
                playlist_id = %s
                AND
                playlist_version = '%s'
                AND
                is_track_position_processed = True
        """
        self.c.execute(query, (playlist_id, playlist_version))

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
                playlist_version = '%s'
                AND
                is_track_position_processed in (True, False)
        """
        self.c.execute(attempted_query, (playlist_id, playlist_version))

        if self.c.fetchone():
            return True
        return False

    # * in argument list delineates positional arguments from enforced keyword arguments.
    # http://www.informit.com/articles/article.aspx?p=2314818
    def set_playlist_processed(self, playlist_id, playlist_version, date_str, *, is_followers_processed = None, is_track_position_processed = None):
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
            ;
        """

        # query_update_followers = """
        #     UPDATE playlist_processed
        #     SET is_followers_processed = %s
        #     WHERE
        #         playlist_id = %s
        #         AND
        #         date_str = %s
        #         AND
        #         is_followers_processed IS DISTINCT FROM %s
        # """
        #
        # query_update_track_position = """
        #     UPDATE playlist_processed
        #     SET is_track_position_processed = %s
        #     WHERE
        #         playlist_id = %s
        #         AND
        #         date_str = %s
        #         AND
        #         is_track_position_processed IS DISTINCT FROM %s
        # """

        try:
            # if is_followers_processed is None and is_track_position_processed is None:
            #     self.c.execute(
            #         query_insert,
            #         (playlist_id, is_followers_processed, is_track_position_processed, date_str)
            #     )
            # # assumption with if/elif flow is that only one of the is_porcessed flags is being set at a time
            # elif is_followers_processed is not None:
            #     print('setting followers processed for ', playlist_id, date_str, is_followers_processed)
            #     self.c.execute(
            #         query_update_followers,
            #         (is_followers_processed, playlist_id, date_str, is_followers_processed)
            #     )
            # elif is_track_position_processed is not None:
            #     self.c.execute(
            #         query_update_track_position,
            #         (is_track_position_processed, playlist_id, date_str, is_track_position_processed)
            #     )
            if is_followers_processed is None and is_track_position_processed is None:
                self.c.execute(
                    query_insert,
                    (playlist_id, playlist_version, date_str)
                )
            elif is_followers_processed and is_track_position_processed is None:
                print('FOLLOWERS PROCESSING')
                self.c.execute(
                    query_upsert_followers.format(playlist_version),
                    (playlist_id, playlist_version, date_str, is_followers_processed,
                    is_followers_processed,
                    playlist_id, date_str)
                )
                print('FOLLOWERS PROCESSED')
            elif is_track_position_processed and is_followers_processed is None:
                self.c.execute(
                    query_upsert_track_position.format(playlist_version),
                    (playlist_id, playlist_version, date_str, is_track_position_processed,
                    is_track_position_processed,
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
                    playlist_id, playlist_version, date_str)
                )

                self.c.execute(
                    query_upsert_track_position,
                    (playlist_id, playlist_version, date_str, is_track_position_processed,
                    is_track_position_processed,
                    playlist_id, playlist_version, date_str)
                )
        # except IndexError:
        #     print('')
        #     continue

        except Exception as e:
            print(e)
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
                    """, (SERVICE_ID, artist_id, genre))

                # update track table
                #
                if not db_track_id:
                    self.c.execute("""
                        INSERT INTO track
                        (service_id, service_track_id, artist_id, album_id, track, isrc)
                        VALUES
                        (%s, %s, %s, %s, %s, %s)
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
                RETURNING id
            """, (SERVICE_ID, db_playlist_id, playlist_version, db_track_id, isrc, position, date_str)
            )

        return True

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
        Retrive service_album_id
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

    def add_playlist(self, playlist):
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

    # updates the db to store latest version of a playlist
    def update_playlist_latest_version(self, playlist):
        self.c.execute("""
            UPDATE playlist
            SET latest_version = %s
            WHERE id = %s
            """,
            (playlist['snapshot_id'], playlist['db_playlist_id'])
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

    def get_playlists_with_minimum_follower_count_from_db(self, followers = 1000, date_of_followers = '2018-01-12'):
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
            playlist['playlist_id'] = row[1]
            playlist['owner_id'] = row[2] # this is the spotify owner id, ie.  a string 'filtr', or 'spotify' NOT the db id
            playlist['snapshot_id'] = row[3] # construct this playlist object as if it were coming from the Spotify API with keys like 'snapshot_id'
            playlists_list.append(playlist)

        return playlists_list

    def get_playlists_track_processed_by_date(self, date_str = TODAY):
        # is_track_position can be true or false if it processed correctly or failed.
        # We check for NOT NULL so that any unprocessed playlists can go, without getting stuck on certain playlists if they fail repeatedly.
        query = """
            SELECT playlist_id from playlist_processed
            WHERE
                date_str = %s
                and
                is_track_position_processed IS NOT NULL
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

    def get_playlists_followers_processed_by_date(self, date_str = TODAY):
        query = """
            SELECT playlist_id from playlist_processed
            WHERE
                date_str = %s
                and
                is_followers_processed IS NOT NULL
        """

        self.c.execute(
            query,
            [date_str]
        )

        playlist_ids = []
        for row in self.c.fetchall():
            playlist_ids.append(row[0])

        return playlist_ids

    def add_playlist_followers(self, service_id, date_str, playlist):
        try:
            db_playlist_id  = playlist['db_playlist_id']
            self.c.execute("""
                INSERT INTO playlist_followers
                (service_id, playlist_id, playlist_version, followers, date_str)
                VALUES
                (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (service_id, db_playlist_id, playlist['snapshot_id'], playlist['followers'], date_str))

            print("{} playlist id's followers added to db".format(db_playlist_id))

        except psycopg2.IntegrityError as e:
            if e.pgcode != '23502': # Not Null constraint: https://www.postgresql.org/docs/8.1/static/errcodes-appendix.html
                print('Postgresql Integrity Error: ', e)
            print('Adding playlist followers to db failed...')

    def get_db_tracks_by_playlist(self, service_playlist_id, playlist_version):
    # get all info from db from most recent date of latest playlist version
    #
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

def get_playlists_by_user(user_id, nextUrl=None):

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
            # validate_playlist_owner(playlists, user_id)
            print('-----{} playlists-----'.format(user_id))
            return playlists
        else:
            print('...getting more playlists for {}'.format(user_id))
            return playlists + get_playlists_by_user(user_id, r['next'])

        return playlists

    except Exception as e:
        logger.warn(e)

def get_all_playlists(users):
    all_playlists = []
    for user in users:
        user_playlists = get_playlists_by_user(user)
        all_playlists += user_playlists
        print('{} playlists:'.format(len(user_playlists)))
        print_divider(len(user_playlists), '*')
    print_divider(40)
    print('TOTAL PLAYLISTS FOR ALL USERS {}'.format(len(all_playlists)))
    return all_playlists

# Playlist Helper functions
#

def validate_playlist_owner(playlists, user_id):
    for p in playlists:
        if p['owner'] != user:
            logger.warning('Spotify user {} has playlist id {} of owner {}, uri: {}'.format(user_id, p['spotify_id'], p['owner'], p['uri']))

def print_playlist_names(playlists):
    for p in playlists:
        print(p['name'])

def print_divider(number, divider='-'):
    print(divider * number)

#
def get_daily_unprocessed_playlists_tracks(date_str = TODAY):
    playlists = db.get_playlists_with_minimum_follower_count_from_db()
    # db_ids_processed = db.get_playlists_track_processed_by_date(date_str)


    # simple way
    db_ids_unprocessed = db.get_playlists_track_not_processed_by_date(date_str)
    unprocessed_playlists = [pl for pl in playlists if pl['db_playlist_id'] in db_ids_unprocessed]
    return unprocessed_playlists

# Strategy of daily unprocessed lists
#
def get_daily_unprocessed_playlists_followers(limit = 150):
    playlists = db.get_playlists_with_minimum_follower_count_from_db()
    db_ids_processed = db.get_playlists_followers_processed_by_date()
    unprocessed_playlists = [pl for pl in playlists if pl['db_playlist_id'] not in db_ids_processed]
    unprocessed_playlists = unprocessed_playlists[0:limit]
    return unprocessed_playlists

# takes a list of playlist objects and removes objects where the playlist db id is in the unwanted list
def temporary_remove_unwanted_playlists(playlists):
    unwanted = LANGUAGE_PLAYLIST_DB_IDS
    wanted = [pl for pl in playlists if pl['db_playlist_id'] not in unwanted]
    return wanted

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
            if not db.is_playlist_followers_processed(playlist['db_playlist_id'], playlist['snapshot_id']):
                owner_id = playlist.get('owner_id')
                followers, snapshot_id = get_followers_for_playlist(owner_id, playlist_id)
                playlists[playlist_id]['followers'] = followers # NOTE: until you change track id to be spotify track id, there will be problems finding the followers
                playlists[playlist_id]['snapshot_id'] = snapshot_id

                if followers and owner_id and playlist_id:
                    print('{} followers for {}:{}'.format(format(followers, ',d'), owner_id, playlist_id))

                # write to db
                db.add_playlist_followers(SERVICE_ID, TODAY, playlist)
                db.update_playlist_latest_version(playlist)

                # mark playlist as followers having been processed
                db.set_playlist_processed(playlist['db_playlist_id'], snapshot_id, TODAY, is_followers_processed = True)

            else:
                print('{} db followers already in db for {}'.format(playlist_id, TODAY))
        except psycopg2.IntegrityError as e:
            print('Adding playlist followers to db failed...')
            continue
    return playlists

def get_followers_for_playlist(user_id, playlist_id):
    query_params = 'fields=followers.total,snapshot_id'
    endpoint = '{}/users/{}/playlists/{}?{}'.format(SPOTIFY_API, user_id, playlist_id, query_params)

    r = spotify.request(endpoint)

    if r and r['followers']:
        followers = r['followers']['total']
        snapshot_id = r['snapshot_id']
        return (followers, snapshot_id)

    return (None, None)

# def test_multiple_playlists(user_id, playlist_id):
#     query_params = 'fields=followers.total,snapshot_id'
#     endpoint = '{}/users/{}/playlists/{}?{}'.format(SPOTIFY_API, user_id, playlist_id, query_params)
#     try:
#         r = spotify.request(endpoint)

def get_tracks_by_playlist(user_id, playlist_id, tracks=[], nextUrl=None):
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
                    print(t)
                    continue
                except Exception as e:
                    logger.warn(e)
                    print(t)
                    continue
            print('{}:{} got all {} tracks'.format(user_id, playlist_id, len(tracks_list)))
            return tracks_list

        # there are more tracks in the playlist, call function recursively
        else:

            print('Retrieving more tracks for playlist {}, running total: {}'.format(playlist_id, len(tracks)))
            return get_tracks_by_playlist(user_id, playlist_id, tracks, r.get('next'))

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

# returns playlist id from db, else creates the playlist and returns the newly created id
def get_db_playlist_info(playlist):

    try:
        # Retrieve id and version from DB
        db_playlist_id, playlist_version = db.get_playlist_info(playlist['playlist_id'])

        # compare playlist version, set flag
        if playlist['snapshot_id'] == playlist_version:
            playlist_version_same = True
            print('{} Spotify playlist versions are the same for playlist id: {}'.format(PRINT_PREFIX, db_playlist_id))
        else:
            playlist_version_same = False

        # if id exists, return. Else, add playlist to db, and return new id
        if db_playlist_id:
            return (int(db_playlist_id), playlist_version_same)
        else:
            return (db.add_playlist(playlist), playlist_version_same)

    except Exception as e:
        print(e)
        return (None, False)
        raise

def append_db_owner_id(playlists):
    for playlist_id, playlist in playlists.items():
        try:
            # spotify service_id = 1, placeholder
            playlist.setdefault('db_owner_id', None)
            playlist['db_owner_id'] = get_db_owner_id(SERVICE_ID, playlist)

        except Exception:
            raise
    return playlists

def append_db_playlist_info(playlists):
    for playlist_id, playlist in playlists.items():
        try:
            playlists[playlist_id].setdefault('db_playlist_id', None)
            db_playlist_id, playlist_version_same = get_db_playlist_info(playlist)
            playlists[playlist_id]['db_playlist_id'] = db_playlist_id
            playlists[playlist_id]['playlist_version_same'] = playlist_version_same

            # if there's a newer version
            if not playlist_version_same:
                db.update_playlist_latest_version(playlist)

        except Exception:
            raise
    return playlists


# if playlist tracks for the playlist version are not in the db, add them

def append_single_playlist_tracks(playlist):
    try:
        tracks_list = []
        tracks_dict = {}

        tracks_list = get_tracks_by_playlist(playlist['owner_id'], playlist['playlist_id'])

        if len(tracks_list) > 0:
            tracks_dict = convert_list_to_dict_by_attribute(tracks_list, 'track_id')
            tracks_dict = append_db_ids(tracks_dict)
            tracks_dict = append_artist_data(tracks_dict)
            tracks_dict = append_album_data(tracks_dict)

            # add the tracks list to the playlist object
            playlist['tracks'] = tracks_dict

            db.add_playlist_tracks(TODAY, playlist['db_playlist_id'], playlist['snapshot_id'], tracks_dict)
            print('{} All tracks added for playlist id {}'.format(PRINT_PREFIX, playlist['db_playlist_id']))
            return True
        else:
            return False

    except TypeError:
        return False

def append_all_playlist_tracks(playlists):
    for playlist_id, playlist in playlists.items():
        append_single_playlist_tracks(playlist)

def append_playlist_tracks(playlists):
    for playlist_id, playlist in playlists.items():
        try:
            # if daily track position is not processed
            if not db.is_playlist_track_position_processed(playlist['db_playlist_id'], playlist['snapshot_id']):
                playlists[playlist_id].setdefault('tracks', {})
                playlists[playlist_id].setdefault('is_track_list_from_db', False) # flag for how to insert tracks into db

                # if latest version is same as db order, grab tracks from db to save API call
                tracks_list = []
                tracks_dict = {}

                # Get the list of tracks from the DB, if the playlist version is the same, rather than from the api
                # NOTE: we will not be writing to the DB when the playlist version is the same. Rather, the tracks for a
                # playlist version will be queried on report runtime generation
                #
                if playlist.get('playlist_version_same'):
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
                    tracks_list = get_tracks_by_playlist(playlist['owner_id'], playlist['playlist_id'])
                    tracks_dict = convert_list_to_dict_by_attribute(tracks_list, 'track_id')
                    # tracks_dict = playlists[playlist_id].setdefault('tracks', {})
                    tracks_dict = append_db_ids(tracks_dict)
                    tracks_dict = append_artist_data(tracks_dict)
                    tracks_dict = append_album_data(tracks_dict)
                    print('Appended new track data...')

                # add the tracks list to the playlist object
                playlists[playlist_id]['tracks'] = tracks_dict

                # write to database, batman
                # db.add_playlist_tracks(TODAY, playlist['db_playlist_id'], playlist['snapshot_id'], tracks_dict, playlist['is_track_list_from_db'])
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

"""
0. Create Playlist TABLE
0. Create Playlist Creator TABLE
0. Create song Playlist table


4. get position:
    https://developer.spotify.com/web-api/get-playlist/
        r.items.xxx.popularity

    https://api.spotify.com/v1/users/{user_id}/playlists/{playlist_id}/tracks
"""

# NOTE: batch update by users once a week or something. Not Used YET!!!
def process_update_all_playlists_by_users(users):
    # INITIALIZE VARIABLES
    service_name = 'Spotify'

    # NOTE: DO NOT GET ALL PLAYLISTS EVERY TIME
    playlists_list = get_all_playlists(users)
    pprint('update_all_playlists_by_users debug: playlist list is this long: {}'.format(len(playlists_list)))

    playlists = convert_list_to_dict_by_attribute(playlists_list, 'playlist_id')
    playlists = append_db_owner_id(playlists)
    playlists = append_db_playlist_info(playlists)

    print('writing to db')
    append_playlist_followers_and_update_version(playlists)

def process_daily_followers(playlists_list):
    starttime_total = datetime.now() # timestamping

    # INITIALIZE VARIABLES - DO I NEED THIS?
    service_name = 'Spotify'

    # getting ready for liftoff! to the moon!
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'playlist_id')

    # need to update latest version and follower count.
    playlists = append_playlist_followers_and_update_version(playlists)

# regular everyday processing of playlist tracks, once all playlists have their tracks processed the first time
# if process_all is True, don't check whether the playlist version is the latest, process all playlists
#
def process_daily_track_position(playlists_list):
    # playlists_list = db.get_playlists_with_minimum_follower_count_from_db(followers = 1000)
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'playlist_id')

    for playlist_id, playlist in playlists.items():

        # update playlist version to latest snapshot id, if necessary
        # NOTE: can rewrite the unprocessed playlists query to grab the playlists with CHANGED playlist versions ONLY
        # playlist['snapshot_id'] is the version of the latest poll of playlist versions and followers (minimum follower count function)
        # db.get_playlist_version is the latest version of the playlist stored in the db, which would be updated to the latest version when the followers
        # count is updated daily.
        #
        if playlist['snapshot_id'] != db.get_playlist_version(playlist):
            db.update_playlist_latest_version(playlist)
            try:
                append_single_playlist_tracks(playlist)
                db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = True)
            except TypeError:
                db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = False)
                # if URLerror returns false, and NoneType is not iterable
                continue

    return 'processed daily track positions for given playlists'

# processes all playlist track positions, does not check for playlist version before processing
#
def periodic_process_daily_track_position(playlists_list):
    # playlists_list = db.get_playlists_with_minimum_follower_count_from_db(followers = 1000)
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'playlist_id')

    for playlist_id, playlist in playlists.items():
        db.update_playlist_latest_version(playlist)

        try:
            did_succeed = append_single_playlist_tracks(playlist)
            if did_succeed:
                db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = True)
            else:
                db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = False)
        except TypeError:
            db.set_playlist_processed(playlist['db_playlist_id'], playlist['snapshot_id'], TODAY, is_track_position_processed = False)
            # if URLerror returns false, and NoneType is not iterable
            continue

    return 'periodic processing complete - processed daily track positions for all playlists'

def process():
    """
    Process each region for "date" mode
    Can be YYYY-MM-DD, "watch", "all", or "latest"
    """
    starttime_total = datetime.now() # timestamping

    # INITIALIZE VARIABLES
    service_name = 'Spotify'

    # 1. GET LIST OF ALL PLAYLISTS BY LIST OF USERS (ids)
    # ---------
    print('GETTING ALL PLAYLISTS FOR {} USERS'.format(len(USERS)))
    print_divider('*', 40)
    # playlists_list = get_all_playlists(USERS)

    #----------GET ONE USERS PLAYLIST -- TESTING ONLY ------------!!!
    playlists_list = get_all_playlists(['filtr'])
    # pprint(playlists_list)
    # TODO:  Does it make sense to grab playlists not from all users? Maybe from the db? YESSSSS
    # playlists = get_relevant_playlists_from_db()

    # 2. DE-DUPLICATE PLAYLISTS
    # playlists may be duplicated by different users. Gather all playlists first from all users, then dedupe them here.
    # ---------
    # print('PLAYLIST LENGTH BEFORE UNIQUE= {}'.format(len(playlists_list)))
    playlists_list = list(unique_everseen(playlists_list, key=lambda e: '{uri}'.format(**e)))
    print('TOTAL UNIQUE PLAYLISTS {}'.format(len(playlists_list)))

    # 3. CONVERT TO USABLE DICTIONARY
    # ---------
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'playlist_id')


    # 4. ADD OWNER OR CREATE OWNER
    # ---------
    starttime = datetime.now() # timestamp

    playlists = append_db_owner_id(playlists)

    endtime = datetime.now()
    processtime = endtime - starttime
    processtime_running_total = endtime - starttime_total
    print('Processing time: %i minutes, %i seconds' % divmod(processtime.days * 86400 + processtime.seconds, 60))
    print('Total time: %i minutes, %i seconds' % divmod(processtime_running_total.days * 86400 + processtime_running_total.seconds, 60))
    #----------TESTING ONLY ------------!!!
    # NOTE: user is TEST_USERS = ['radioactivehits']
    # playlist_id_single = '2JmG2Y8eqoKGtDaNUcnAks'
    # playlists = {playlist_id_single: playlists[playlist_id_single]}


    # 5. ADD PLAYLIST OR CREATE PLAYLIST
    starttime = datetime.now() # timestamp

    playlists = append_db_playlist_info(playlists)

    endtime = datetime.now()
    processtime = endtime - starttime
    processtime_running_total = endtime - starttime_total
    print('Processing time: %i minutes, %i seconds' % divmod(processtime.days * 86400 + processtime.seconds, 60))
    print('Total time: %i minutes, %i seconds' % divmod(processtime_running_total.days * 86400 + processtime_running_total.seconds, 60))

    # 6. COMPARE AND UPDATE LATEST VERSION
    # NOTE: insert new 'latest_version' for playlist_track_position
    # https://stackoverflow.com/questions/3634984/insert-if-not-exists-else-update

    #----------TESTING ONLY ------------!!!
    # import random
    # random_id = random.choice(list(playlists))
    # print('TAKING A RANDOM PLAYLIST...{}'.format(random_id))
    # playlists = {random_id: playlists[random_id]}

    # 7. GET FOLLOWERS FOR ALL PLAYLISTS
    # 8. ADD FOLLOWERS TO DB
    starttime = datetime.now() # timestamp

    playlists = append_playlist_followers_and_update_version(playlists)

    endtime = datetime.now()
    processtime = endtime - starttime
    processtime_running_total = endtime - starttime_total
    print('Processing time: %i minutes, %i seconds' % divmod(processtime.days * 86400 + processtime.seconds, 60))
    print('Total time: %i minutes, %i seconds' % divmod(processtime_running_total.days * 86400 + processtime_running_total.seconds, 60))


    # 9. APPEND TRACKS TO PLAYLIST OBJECT FROM API OR DB
    starttime = datetime.now() # timestamp

    playlists = append_playlist_tracks(playlists)

    endtime = datetime.now()
    processtime = endtime - starttime
    processtime_running_total = endtime - starttime_total
    print('Processing time: %i minutes, %i seconds' % divmod(processtime.days * 86400 + processtime.seconds, 60))
    print('Total time: %i minutes, %i seconds' % divmod(processtime_running_total.days * 86400 + processtime_running_total.seconds, 60))


    # timestamping
    endtime_total = datetime.now()
    processtime_total = endtime_total - starttime_total
    print('Finished processing at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
    print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days * 86400 + processtime_total.seconds, 60))
    print('-' * 40)


### AWS LAMBDA HANDLERS
#
#
#
#
#

def handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # prompt for date/mode
    process()

    db.show_table_stats()

    # clean up
    db.close_database()
    print('closed database connection')
    return 'finished spotify'

def periodic_update_all_playlists_by_users_handler(event, context):
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

    process_update_all_playlists_by_users(users)

    # clean up
    db.close_database()
    print('closed database connection')
    return 'finished spotify updating of all playlists'

def daily_followers_handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # input is a list of playlist objects from the db
    playlists_list = event

    # prompt for date/mode
    process_daily_followers(playlists_list)

    db.show_table_stats()

    # clean up
    db.close_database()
    print('closed database connection')
    return 'finished playlist followers'


def daily_track_position_handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # input is a list of playlist objects from the db
    playlists_list = event

    process_daily_track_position(playlists_list)

    db.close_database()
    print('closed database connection')
    return 'finished playlist track positions'

def get_playlists_with_minimum_follower_count_from_db_handler(event, context):
    global db
    db = TrackDatabase()

    playlists = db.get_playlists_with_minimum_follower_count_from_db()

    db.close_database()

    return playlists


# Lambda input should have { "should_process_date": true, "date_str": "2018-01-18"}
# The issue with this strategy is that with the same list of failing (either from timeouts or otherwise) playlists
# come up time and again.
#
def get_daily_unprocessed_playlists_tracks_handler(event, context):
    global db
    db = TrackDatabase()

    # if given date to process, use it
    if event.get('should_process_date'):
        unprocessed_playlists = get_daily_unprocessed_playlists_tracks(event.get('date_str'))
    else:
        unprocessed_playlists = get_daily_unprocessed_playlists_tracks()

    batch_playlists = temporary_remove_unwanted_playlists(unprocessed_playlists)[0:15]
    print(batch_playlists)

    db.close_database()
    return batch_playlists


def unprocessed_track_playlists(event, context):
    global db
    db = TrackDatabase()
    min_follower_pl = db.get_playlists_with_minimum_follower_count_from_db()

    print('min followers', len(min_follower_pl))
    # filter out playlists... if it's been attempted (true or false, false if it failed for some reason like URLError)
    # can also try using is_playlist_track_position_processed()
    #
    unprocessed = [pl for pl in min_follower_pl if not db.is_playlist_track_position_attempted(pl['db_playlist_id'], pl['snapshot_id'])]
    print(len(unprocessed), unprocessed)

    batch = temporary_remove_unwanted_playlists(unprocessed)[0:15]

    db.close_database()

    return batch

def get_daily_unprocessed_playlists_followers_handler(event, context):
    global db
    db = TrackDatabase()

    # NOTE: TODO POTENTIAL create lambda function to dynamically change the limit. This will also entail creating
    # a function to dynamically evenly break up the list into 3 even sized chunks, and for the parallel functions
    # to grab the value off the input from the lambda... It may be too much trouble, and may not change as much as you think!
    # If you do implement it, it will be simple like this:
    # limit = event.playlist_limit


    unprocessed_playlists = get_daily_unprocessed_playlists_followers()
    unprocessed_playlists = temporary_remove_unwanted_playlists(unprocessed_playlists)

    db.close_database()

    return unprocessed_playlists

def periodic_process_daily_track_position_handler(event, context):
    global spotify
    global db

    # setup db
    db = TrackDatabase()

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # input is a list of playlist objects from the db
    playlists_list = event

    periodic_process_daily_track_position(playlists_list)

    db.close_database()
    print('closed database connection')
    return 'finished playlist track positions'
