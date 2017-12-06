import sqlite3, csv, codecs, re, json, os, base64, time, hashlib, ssl, datetime, logging, errno
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from socket import error as SocketError
from lxml import html
from pprint import pprint as pprint

logger = logging.getLogger('spotify_api')
hdlr = logging.FileHandler('./spotify/playlist_spotify.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

PRINT_PREFIX = '++'

# cache http requests?
CACHE_ENABLED = False

# spotify app client ID
CLIENT_ID = 'e021413b59f5430d9b1b0b46f67c9dec'

# spotify app client secret
CLIENT_SECRET = '1c155d57d1514944972ea4a6b7ed7554'

DATABASE_NAME = 'v9playlist.db'

# sqlite database filename/path
DATABASE_FILE = '../{}'.format(DATABASE_NAME)

# the daily regional CSV download link
CSV_url = 'https://spotifycharts.com/regional/{}/daily/{}/download'

# the regions to download
# All REGIONS
REGIONS_TOTAL = [
    'global', 'us', 'gb','ad','ar','at','au','be','bo','br',
    'ca','ch','cl','co','cr','cz','de','dk','do','ec',
    'ee','es','fi','fr','gr','gt','hk','hn','hu','id','ie',
    'is','it','jp','lt','lu','lv','mc','mt','mx','my','ni',
    'nl','no','nz','pa','pe','ph','pl','pt','py','se','sg',
    'sk','sv','th','tr','tw','uy'
]

# NOTE: add to this list as more regions are discovered without daily downloads
REGIONS_WITHOUT_DAILY = ['bg', 'cy', 'ni']

REGIONS = list(set(REGIONS_TOTAL).difference(REGIONS_WITHOUT_DAILY))

SPOTIFY_API = 'https://api.spotify.com/v1'

# Spotify Users Playlists
SPOTIFY_USERS = ['spotify', 'topsify', 'filtr']
UNIVERSAL_USERS = ['radioactivehits', 'digster.ee', 'digster.dk', 'dgdeccaclassics', 'digster.co.uk', 'hhheroes',
'digster.lt', 'capitolchristianmusicgroup', 'capitolrecords', 'digsterca', '11145233736', '12150271040',
'digsterdeutschland', 'digstercz', 'peacefulclassics', 'digster.lv', 'sinfinimusic', 'hollywdrecrds', 'record_club_umc',
'sozoofficial', '116734391', 'digsterhu', 'getmusicasia', 'disney_pixar_', 'digstersk', 'deutschegrammophon', '11152361853',
'100keepit', 'universal.fm', 'digsternl', '12150809594', 'thisisofficial', 'universalmusicargentina', 'universalmusicse',
'udiscover', 'umusicnz', 'universalmusicitalia', 'progrocksmusic', 'thecompletecollection', 'digsterargentina', 'abbaspotify',
'defjamrecordings', 'digster.fr', 'digsterno', 'digster.au', '100yearsoftheblues', 'universal.pictures.de', 'o.owner_id', '128899670',
'digstergreece', 'universalmusica', 'digster.fi', 'digster.se', 'universalmusictaiwan', 'classicmotownrecords',
'digster_italy', 'digster_brasil', 'thejazzlabels', 'universalmusicireland', 'wowilovechristianmusic', 'sinfinimusic.nl', 'digster.fm',
'digsterchile', 'disney_music_uk', 'udiscovermusic', 'universal_music_rock_legends', 'digster.pt']

TEST_USERS = ['radioactivehits']

MESSEDUP_UNIVERSAL_USERS = ['el_listÃ³n', 'digstertÃ¼rkiye']

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

def get_dates_for_region(region):
    """
    Scrape the available chart dates for a given region
    Returns a list of date strings
    """
    # scrape xpath from html page div structure
    url = 'https://spotifycharts.com/regional/{}/daily/'.format(region)
    r = get_page(url)
    page = html.fromstring(r)
    xpath = '*//div[contains(concat(" ", normalize-space(@data-type), " "), " date ")]/ul/li/text()'
    rows = page.xpath(xpath)
    # check that rows is valid
    if not isinstance(rows, list) and not len(rows):
        return False
    # convert M/D/Y to Y-M-D
    return [re.sub(r"(\d{2})\/(\d{2})\/(\d{4})", '\\3-\\1-\\2', d, 0) for d in rows]

def get_spotify_csv_url(region, date='latest'):
    return CSV_url.format(region, date)

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
            print('URLError... ' + str(e.reason))
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
    Appends key 'track_id_db' key with db lookup value.
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
        AND service_track_id = (?)
    """
    # NOTE: there should be a one-to-one relationship between spotify trackId and db id
    row = db.c.execute(query, [track['track_id']]).fetchone()

    if row:
        track['db_track_id'] = row[0]

    return track

def append_db_artist_id(track):
    track.setdefault('db_artist_id', None)

    query = """
        SELECT id
        FROM artist
        WHERE service_id = 1
        AND service_artist_id = (?)
    """
    # NOTE: there should be a one-to-one relationship between spotify trackId and db id
    row = db.c.execute(query, [track['artist_id']]).fetchone()

    if row:
        track['db_artist_id'] = row[0]

    return track

def append_db_album_id(track):
    track.setdefault('db_album_id', None)

    query = """
        SELECT id
        FROM album
        WHERE service_id = 1
        AND service_album_id = (?)
    """
    # NOTE: there should be a one-to-one relationship between spotify trackId and db id
    row = db.c.execute(query, [track['album_id']]).fetchone()

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
            id_str = ','.join(batch)
            r = spotify.request(endpoint_albums.format(id_str))
            r_list += r['albums']
            print('Retrieved album data {} of {} batches'.format(i + 1, len(batches)))

        album_data_dict = convert_list_to_dict_by_attribute(r_list, 'id')

        for track_id, track in tracks.items():
            if not track['db_album_id']:
                tracks[track_id]['album_release_date'] = album_data_dict[track['album_id']]['release_date']
                tracks[track_id]['album_label'] = album_data_dict[track['album_id']]['label']
        print('Appended all album data to tracks list')
    else:
        album_id = albums[0]
        album = spotify.request(endpoint_album.format(album_id))
        for track_id, track in tracks.items():
            if not track['db_album_id']:
                tracks[track_id]['release_date'] = album['release_date']
                tracks[track_id]['label'] = album['label']
        print('ADDED all ALBUM data to tracks list')
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
        id_str = ','.join(batch)
        r = spotify.request(endpoint.format(id_str))
        r_list += r['artists']
        print('Retrieved artist data {} of {} batches'.format(i + 1, len(batches)))
    artist_data_dict = convert_list_to_dict_by_attribute(r_list, 'id')

    for track_id, track in tracks.items():
        if not track['db_artist_id']:
            tracks[track_id].setdefault('genres', [])
            tracks[track_id]['genres'] = artist_data_dict[track['artist_id']]['genres']
    print('ADDED all ARTIST data to tracks list' )
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
    """ SQLite Database Manager """
    def __init__(self, db_file=DATABASE_NAME):
        super(TrackDatabase, self).__init__()
        self.db_file = db_file
        self.init_database()

    def init_database(self):
        print('Initializing database...')
        self.db = sqlite3.connect(self.db_file)
        self.c = self.db.cursor()

        #track table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS track (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                service_track_id text NOT NULL,
                artist_id integer NOT NULL,
                album_id integer NOT NULL,
                track text NOT NULL,
                isrc text NOT NULL
            )
        ''')

        # artist table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS artist (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                service_artist_id text NOT NULL,
                artist text NOT NULL
            )
        ''')

        # artist_genre mapping table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS artist_genre (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                artist_id integer NOT NULL,
                genre text NOT NULL
            )
        ''')

        self.c.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS artist_genre_serviceId_artistId_genre ON artist_genre (service_id, artist_id, genre)
        ''')

        # album table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS album (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                service_album_id text NOT NULL,
                artist_id integer NOT NULL,
                album text NOT NULL,
                label text NOT NULL,
                release_date text NOT NULL
            )
        ''')

        # music video table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS music_video (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                service_music_video_id integer NOT NULL,
                music_video text NOT NULL
            )
        ''')

        # processed table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS processed (
                url text PRIMARY KEY
            )
        ''')

        # service table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS service (
                id integer PRIMARY KEY,
                service_name text NOT NULL
            )
        ''')
        self.c.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS service_name_u ON service (service_name)
        ''')
        # stats table
        # NOTE: may want to add isrc field as well for better query
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS peak_track_position (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                territory_id integer NOT NULL,
                track_id integer NOT NULL,
                first_added text NOT NULL,
                last_seen text NOT NULL,
                peak_rank integer NOT NULL,
                peak_date text NOT NULL
            )
        ''')
        self.c.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS peak_track_position_service_territory_track_ids_u ON peak_track_position (service_id, territory_id, track_id)
        ''')

        # track_position table
        # isrc is denormalized duplicated from track table for better lookup
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS track_position (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                territory_id integer NOT NULL,
                track_id integer NOT NULL,
                isrc text NOT NULL,
                position integer NOT NULL,
                stream_count integer NOT NULL DEFAULT 0,
                date_str text NOT NULL
            )
        ''')

        # territory table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS territory (
                id integer PRIMARY KEY,
                code varchar(10) NOT NULL,
                name text NOT NULL
            )
        ''')

        self.c.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS territory_code_u ON territory (code)
        ''')

        # SEED DATABASE TABLES
        #
        # seed service table
        self.c.execute('''
            INSERT OR IGNORE INTO service
            (service_name)
            VALUES
            (?)
        ''', (['Spotify'])
        )

        self.c.execute('''
            INSERT OR IGNORE INTO territory
            (code, name)
            VALUES
            (?, ?)
        ''', ('global', 'global')
        )

        self.db.commit()

        stat = os.stat(self.db_file)
        print("Using Database '%s'" % self.db_file)
        print("# Bytes: %r" % stat.st_size)
        bq = self.c.execute("SELECT COUNT(*) FROM processed")
        print("# urls Processed: %r" % bq.fetchone()[0])
        tq = self.c.execute("SELECT COUNT(*) FROM track")
        print("# Tracks: %r" % tq.fetchone()[0])
        sq = self.c.execute("SELECT COUNT(*) FROM peak_track_position")
        print("# Track Stats: %r" % sq.fetchone()[0])
        pq = self.c.execute("SELECT COUNT(*) FROM track_position")
        print("# Position Stats: %r\n" % pq.fetchone()[0])

    def is_processed(self, url):
        """
        Has CSV url already been processed?
        """
        query = '''
            SELECT * FROM processed WHERE url = ?
        '''
        for row in self.c.execute(query, [url]):
            return True
        return False

    def set_processed(self, url):
        """
        Mark url as already processed
        """
        try:
            self.c.execute('''
                INSERT OR IGNORE INTO processed
                (url)
                VALUES
                (?)
            ''', [url])
            self.db.commit()
        except Exception as e:
            raise e
        return True

    def get_track_stats(self, service_id, territory_id, track_id):
        """
        Returns a tuple of track stats (track_id, territory_id, service_id, added, last_seen, peak_rank, peak_date)
        """
        query = self.c.execute('''
            SELECT
                first_added,
                last_seen,
                peak_rank,
                peak_date
            FROM peak_track_position
            WHERE
                service_id = ?
            AND
                territory_id = ?
            AND
                track_id = ?
        ''', [service_id, territory_id, track_id])
        row = query.fetchone()
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

        stats_update_query = '''
            UPDATE peak_track_position SET
                first_added = ?,
                last_seen = ?,
                peak_rank = ?,
                peak_date = ?
            WHERE
                service_id = ?
            AND
                territory_id = ?
            AND
                track_id = ?
        '''

        stats_query = '''
            INSERT OR IGNORE INTO peak_track_position
            (service_id, territory_id, track_id, first_added, last_seen, peak_rank, peak_date)
            VALUES
            (?, ?, ?, ?, ?, ?, ?)
        '''

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

    def add_playlist_tracks(self, date_str, service_id, playlist_id, playlist_version, track_list):
        """
        input:
            track_list: dict of all songs to add
        Add tracks to the database
        """
        print('Spotify Playlist {}: THERE ARE {} TRACKS to INSERT '.format(playlist_id, len(track_list)))

        for track_id, track in track_list.items():

            position = track['position']
            isrc = track.setdefault('isrc', None)

            # db_track_id = None

            if track.get('db_track_id'):
                db_track_id = track['db_track_id']

            else:

                try:
                    # check if artist or album are in the db
                    artist_name = track['artist_name']
                    service_album_id = track.get('album_id')
                    service_artist_id = track.get('artist_id')
                    artist_id = track['db_artist_id']
                    album_id = track['db_album_id']
                    # add artist if not in the db
                    if not artist_id:
                        # add artist
                        self.c.execute('''
                            INSERT OR IGNORE INTO artist
                            (service_id, service_artist_id, artist)
                            VALUES
                            (?, ?, ?)
                        ''', (service_id, service_artist_id, artist_name))
                        artist_id = self.c.lastrowid
                        print('Artist added: {}'.format(artist_name))

                    # add album if not in the db
                    if not album_id:
                        self.c.execute('''
                            INSERT OR IGNORE INTO album
                            (service_id, artist_id, service_album_id, album, release_date, label)
                            VALUES
                            (?, ?, ?, ?, ?, ?)
                        ''', (service_id, artist_id, service_album_id, str(track['album_name']), str(track['album_release_date']), str(track['album_label']) )
                        )
                        album_id = self.c.lastrowid
                        print('Album added: {} for {}'.format(str(track['album_name']), artist_name))

                    # add genres for artist
                    if track.get('genres'):
                        for genre in track.get('genres'):
                            self.c.execute('''
                                INSERT OR IGNORE INTO artist_genre
                                (service_id, artist_id, genre)
                                VALUES
                                (?, ?, ?)
                            ''', (service_id, artist_id, genre))

                    # update track table
                    #
                    self.c.execute('''
                        INSERT OR IGNORE INTO track
                        (service_id, service_track_id, artist_id, album_id, track, isrc)
                        VALUES
                        (?, ?, ?, ?, ?, ?)
                    ''',
                        (service_id, track_id, artist_id, album_id, str(track['track_name']), isrc)
                    )
                    print('Track added: {} by {}'.format(str(track['track_name']), artist_name))

                    db_track_id = self.c.lastrowid

                except Exception as e:
                    print('Exception...', e)
                    raise

            # update track_position table
            self.c.execute('''
                INSERT OR IGNORE INTO playlist_track_position
                (service_id, playlist_id, playlist_version, track_id, isrc, position, date_str)
                VALUES
                (?, ?, ?, ?, ?, ?, ?)
            ''', (service_id, playlist_id, playlist_version, db_track_id, isrc, position, date_str)
            )

            self.db.commit()

        return True

    def get_isrc_from_db(self, track_id):
        # RETRIEVE ISRC
        query = """
            SELECT isrc
            FROM track
            WHERE id = ?
        """
        # NOTE: there should be a one-to-one relationship between spotify trackId and db id
        row = db.c.execute(query, [track_id]).fetchone()
        return row[0] if row else False

    def get_territory_id(self, code):
        """
        Retrieve territory_id from region code
        """
        query = self.c.execute('''
            SELECT id FROM territory WHERE code = ?
        ''', [code.lower()])

        row = query.fetchone()
        return row[0] if row else False

    def get_service_id(self, service_name):
        """
        Retrieve service_id from service name
        """
        query = self.c.execute('''
            SELECT id FROM service WHERE service_name = ?
        ''', [service_name])
        row = query.fetchone()
        return row[0] if row else False

    def get_artist_id(self, service_id, service_artist_id):
        """
        Retrive service_artist_id
        """
        query = self.c.execute('''
            SELECT id FROM artist
            WHERE
            service_id = ?
            AND
            service_artist_id = ?
        ''', (str(service_id), service_artist_id)
        )
        row = query.fetchone()
        return row[0] if row else False

    def get_album_id(self, service_id, service_album_id):
        """
        Retrive service_album_id
        INPUT:
            service_id: integer
            service_album_id: text
        """
        query = self.c.execute('''
            SELECT id FROM album
            WHERE
            service_id = ?
            AND
            service_album_id = ?
        ''', (str(service_id), service_album_id)
        )
        row = query.fetchone()
        return row[0] if row else False

    def get_owner_id(self, service_id, service_owner_id):
        """
        Retrive db owner_id based on service_owner_id, ie. Spotify's owner id for the playlists
        Or Apple's curator for their playlist
        INPUT:
            service_id: integer
            service_owner_id: text
        """
        query = self.c.execute('''
            SELECT id FROM playlist_owner
            WHERE
            service_id = ?
            AND
            service_owner_id = ?
        ''', (str(service_id), service_owner_id)
        )
        row = query.fetchone()
        return row[0] if row else False

    def get_playlist_id(self, service_id, service_playlist_id):
        query = self.c.execute('''
            SELECT id FROM playlist
            WHERE
            service_id = ?
            AND
            service_playlist_id = ?
        ''', (str(service_id), service_playlist_id)
        )
        row = query.fetchone()
        return row[0] if row else False

    def add_playlist_owner(self, service_id, playlist):
        '''
        Adds playlist owner to db, returns db owner id
        '''
        self.c.execute('''
            INSERT OR IGNORE INTO playlist_owner
            (service_id, service_owner_id, alt_name)
            VALUES
            (?, ?, ?)
        ''', (service_id, playlist['owner_id'], playlist['owner_display_name']))
        self.db.commit()
        db_owner_id = self.c.lastrowid

        return db_owner_id

    def add_playlist(self, service_id, playlist):
        '''
        Adds playlist owner to db, returns db owner id
        '''
        self.c.execute('''
            INSERT OR IGNORE INTO playlist
            (service_id, service_playlist_id, name, owner_id, latest_version)
            VALUES
            (?, ?, ?, ?, ?)
        ''', (service_id, playlist['playlist_id'], playlist['name'], playlist['db_owner_id'], playlist['snapshot_id']))
        self.db.commit()
        db_playlist_id = self.c.lastrowid
        return db_playlist_id

    def add_playlist_followers(self, service_id, date_str, playlist):
        self.c.execute('''
            INSERT OR IGNORE INTO playlist_followers
            (service_id, playlist_id, playlist_version, followers, date_str)
            VALUES
            (?, ?, ?, ?, ?)
        ''', (service_id, playlist['db_playlist_id'], playlist['snapshot_id'], playlist['followers'], date_str))
        self.db.commit()

# recursively retrieve all playlists from a users
# NOTE: This recursively adds more and more to the final list, which is why we need to use 'list(unique_everseen' method
#
# def get_playlists_by_user(user_id, playlists=[], nextUrl=None):
#
#     if not nextUrl:
#         endpoint = "{}/users/{}/playlists?limit={}".format(SPOTIFY_API, user_id, MAX_LIMIT_QUERY)
#     else:
#         endpoint = nextUrl
#
#     try:
#         r = spotify.request(endpoint)
#
#         # playlists = playlists
#
#         for p in r['items']:
#             playlist = {}
#             playlist['playlist_id'] = p['id']
#             playlist['owner_id'] = p['owner']['id']
#             playlist['owner_display_name'] = p['owner'].get('display_name', '')
#             playlist['name'] = p['name']
#             playlist['snapshot_id'] = p['snapshot_id']
#             playlist['num_tracks'] = p['tracks']['total']
#             playlist['href'] = p['href']
#             playlist['uri'] = p['uri']
#             playlists.append(playlist)
#
#         # if there are more values to return, request more and add to the list
#         if not r['next']:
#             # NOTE: As the playlist continues growing with new users, this deprecated method does not work as expected.
#             # This playlist length validates the current user_id's playlist count with the total running count of all playlists.
#             # validate_playlist_length(user_id, len(playlists), r['total'])
#             validate_playlist_owner(playlists, user_id)
#             print('{} Retrieved all playlists for {}'.format(PRINT_PREFIX, user_id))
#             return playlists
#         else:
#             print('Retrieving more playlists for {}, running total: {}'.format(user_id, len(playlists)))
#             return get_playlists_by_user(user_id, playlists, r['next'])
#
#         return playlists
#
#     except Exception as e:
#         logger.warn(e)
#         return playlists

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
            # NOTE: As the playlist continues growing with new users, this deprecated method does not work as expected.
            # This playlist length validates the current user_id's playlist count with the total running count of all playlists.
            # validate_playlist_length(user_id, len(playlists), r['total'])
            validate_playlist_owner(playlists, user_id)
            print('{} Retrieved all playlists for {}'.format(PRINT_PREFIX, user_id))
            return playlists
        else:
            print('Retrieving more playlists for {}, running total: {}'.format(user_id, len(playlists)))
            return playlists + get_playlists_by_user(user_id, r['next'])

        return playlists

    except Exception as e:
        logger.warn(e)
        return playlists

def get_all_playlists(users):
    all_playlists = []
    for user in users:
        playlists = get_playlists_by_user(user)
        all_playlists = all_playlists + playlists
        print('{} playlists for {}'.format(len(playlists), user))
    print('total playlists {}'.format(len(all_playlists)))
    return all_playlists

# Playlist Helper functions
#
def validate_playlist_length(user_id, length, expected):
    if(length != expected):
        logger.warn('For user {}, did not retrieve all playlists. Have {} of {}'.format(user_id, length, expected))
    else:
        print('{} Retrieved all {}\'s {} playlists!'.format(PRINT_PREFIX, user_id, length))

def validate_playlist_owner(playlists, user_id):
    for p in playlists:
        if p['owner'] != user:
            logger.warn('Spotify user {} has playlist id {} of owner {}, uri: {}'.format(user_id, p['spotify_id'], p['owner'], p['uri']))

def print_playlist_names(playlists):
    for p in playlists:
        print(p['name'])

# Add follower count for playlists
# INPUT: playlists object
#
def append_playlist_data(playlists):
    for playlist_id, playlist in playlists.items():
        owner_id = playlist['owner_id']
        followers = getFollowersForPlaylist(owner_id, playlist_id)
        playlists[playlist_id]['followers'] = followers
        print('{}:{} has {} followers today'.format(owner_id, playlist_id, format(followers, ',d')))
    return playlists

def getFollowersForPlaylist(user_id, playlist_id):
    query_params = 'fields=followers.total,snapshot_id'
    endpoint = '{}/users/{}/playlists/{}?{}'.format(SPOTIFY_API, user_id, playlist_id, query_params)
    try:
        r = spotify.request(endpoint)
        if r:
            followers = r['followers']['total']
            return followers
    except:
        logger.warn('There is no follower count for user: {} and  playlist id: {}'.format(user_id, playlist_id))
        return None
    return None

# def test_multiple_playlists(user_id, playlist_id):
#     query_params = 'fields=followers.total,snapshot_id'
#     endpoint = '{}/users/{}/playlists/{}?{}'.format(SPOTIFY_API, user_id, playlist_id, query_params)
#     try:
#         r = spotify.request(endpoint)

def get_tracks_by_playlist(user_id, playlist_id, tracks=[], nextUrl=None):
    '''
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
    '''
    max_query = 100
    if not nextUrl:
        endpoint = "{}/users/{}/playlists/{}/tracks?limit={}".format(SPOTIFY_API, user_id, playlist_id, max_query)
    else:
        endpoint = nextUrl

    try:
        r = spotify.request(endpoint)
        tracks = tracks + r['items']

        # finished receiving all tracks for playlist
        if not r['next']:
            tracks_list = []
            for idx, t in enumerate(tracks):
                track = {}
                track['position'] = idx + 1
                try:
                    track['artist_id'] = t['track']['album']['artists'][0]['id']
                except IndexError as e:
                    logger.warn('Track {} does not has artist id attached'.format(t['track']['id']))
                    track['artist_id'] = None

                try:
                    track['artist_name'] = t['track']['album']['artists'][0]['name'] # NOTE: take only first artist, as is primary artist, not all collaborators
                except IndexError as e:
                    logger.warn('Track {} does not has artist name attached'.format(t['track']['id']))
                    track['artist_name'] = ''

                track['album_id'] = t['track']['album'].get('id')
                track['album_name'] = t['track']['album'].get('name')
                track['isrc'] = t['track']['external_ids'].get('isrc')
                track['track_id'] = t['track']['id']
                track['track_name'] = t['track']['name']
                track['track_uri'] = t['track']['uri']
                track['popularity'] = t['track']['popularity']
                tracks_list.append(track)

                # DEBUG:
                print('https://open.spotify.com/user/{}/playlist/{}  -- {} in position {}'.format(user_id, playlist_id, track['track_name'], track['position']))
            print('{}:{} got all {} tracks'.format(user_id, playlist_id, len(tracks_list)))
            return tracks_list

        # there are more tracks in the playlist, call function recursively
        else:

            print('Retrieving more tracks for playlist {}, running total: {}'.format(playlist_id, len(tracks)))
            return get_tracks_by_playlist(user_id, playlist_id, tracks, r['next'])

    except Exception as e:
        logger.warn(e)
        raise


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
def get_db_playlist_id(service_id, playlist):

    try:
        db_playlist_id = db.get_playlist_id(service_id, playlist['playlist_id']) # spotify service_id = 1, placeholder

        if db_playlist_id:
            return int(db_playlist_id)
        else:
            return db.add_playlist(service_id, playlist)

    except Exception as e:
        print(e)
        raise
        return None

def append_db_owner_id(service_id, playlists):
    for playlist_id, playlist in playlists.items():
        try:
            # spotify service_id = 1, placeholder
            playlist.setdefault('db_owner_id', None)
            playlist['db_owner_id'] = get_db_owner_id(service_id, playlist)

            # ORIGINAL CODE
            # playlist['db_owner_id'] = get_owner_id(1, playlist['owner_id'])
            #
            # if db_owner_id:
            #     playlist['db_owner_id'] = db_owner_id
            # else:
            #     db.add_playlist_owner()

        except Exception:
            raise
    return playlists

def append_db_playlist_id(service_id, playlists):
    for playlist_id, playlist in playlists.items():
        try:
            playlist.setdefault('db_playlist_id', None)
            playlist['db_playlist_id'] = get_db_playlist_id(service_id, playlist)

        except Exception:
            raise
    return playlists

def append_playlist_tracks(playlists):
    for playlist_id, playlist in playlists.items():
        try:
            playlists[playlist_id].setdefault('tracks', {})
            tracks_list = get_tracks_by_playlist(playlist['owner_id'], playlist_id)
            tracks_dict = convert_list_to_dict_by_attribute(tracks_list, 'track_id')
            playlists[playlist_id]['tracks'] = tracks_dict
        except Exception as e:
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
def process():
    """
    Process each region for "date" mode
    Can be YYYY-MM-DD, "watch", "all", or "latest"
    """
    starttime_total = datetime.datetime.now() # timestamping


    service_name = 'Spotify'
    service_id = 1

    today = datetime.date.today().strftime('%Y-%m-%d')

    # 1. GET LIST OF USER PLAYLISTS (ids)
    playlists_list = get_all_playlists(TEST_USERS)

    print('PLAYLIST LENGTH BEFORE UNIQUE= {}'.format(len(playlists_list)))
    playlists_list = list(unique_everseen(playlists_list, key=lambda e: '{uri}'.format(**e)))
    print('PLAYLIST LENGTH AFTER UNIQUE = {}'.format(len(playlists_list)))

    # 1x. CONVERT TO DICTIONARY
    playlists = convert_list_to_dict_by_attribute(playlists_list, 'playlist_id')

    # 1XX. deduplicate based on uris
    # QUESTION: How often to re-scrape playlists from users?

    # QUESTION How often do the snapshot_ids change? Do they change with follower count too, or just on track / ordering of tracks change'''
    # #TODO: 1a. insert playlists into DB
    #     -compare uris against database URIs or ids
    #     -check to see if playlist snapshot ID is the same as in DB...

    # DB --- ADD OWNERS, or CREATE OWNER
    playlists = append_db_owner_id(service_id, playlists)

    # DB --- ADD PLAYLISTS TO DB
    playlists = append_db_playlist_id(service_id, playlists)

    # NOTE: INSERT HERE:
    # take playlists from db, create object structure simulating other stuff
    # get tracks, append tracks key
    # 3a append FOLLOWERS
    # 4. add followers to db
    # Append playlist tracks
    # add to db
    # playlists = get_relevant_playlists_from_db()


    # NOTE: insert new 'latest_version' for playlist_track_position

    #----------TESTING ONLY ------------!!!
    import random

    random_id = random.choice(list(playlists))
    print('TAKING A RANDOM PLAYLIST...{}'.format(random_id))
    playlists = {random_id: playlists[random_id]}

    # 3a. GET FOLLOWERS FOR ALL PLAYLISTS
    playlists = append_playlist_data(playlists)

    # 4. ADD FOLLOWERS TO DB
    for playlist_id, playlist in playlists.items():
        db.add_playlist_followers(service_id, today, playlist)

    # Append tracks to each playlist
    playlists = append_playlist_tracks(playlists)

    for playlist_id, playlist in playlists.items():
        tracks = playlists[playlist_id].setdefault('tracks', {})
        tracks = append_db_ids(tracks)
        tracks = append_album_data(tracks)
        tracks = append_artist_data(tracks)
        tracks = append_album_data(tracks)
        #
        playlists[playlist_id]['tracks'] = tracks
        print('*' * 40)
        db.add_playlist_tracks(today, service_id, playlist['db_playlist_id'], playlist['snapshot_id'], playlists[playlist_id]['tracks'])
        print('{} All tracks added for playlist id {}'.format(PRINT_PREFIX, playlist['db_playlist_id']))
        print('*' * 40)

    # pprint(playlists, depth=4)

    # timestamping
    endtime_total = datetime.datetime.now()
    processtime_total = endtime_total - starttime_total
    print('Finished processing at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
    print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days *86400 + processtime_total.seconds, 60))
    print('-' * 40)

if __name__ == '__main__':

    # are http requests being cached?
    #CACHE_ENABLED = True
    cache_msg = '\033[92m enabled' if CACHE_ENABLED else '\033[91m disabled'
    print('HTTP cache is%s\033[0m' % cache_msg)
    print('Database file is {}'.format(DATABASE_FILE))

    # setup db
    db = TrackDatabase(DATABASE_FILE)

    # setup Spotify auth and client for API calls
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)

    # prompt for date/mode
    process()

    print('COMPLETE')
