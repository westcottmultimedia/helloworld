import csv, codecs, re, json, os, base64, time, hashlib, ssl, datetime, psycopg2
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
import urllib
from socket import error as SocketError
import errno
# import lxml
import logging
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup as bs

logger = logging.getLogger()
# hdlr = logging.FileHandler('./spotify/spotify_api.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# hdlr.setFormatter(formatter)
# logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# cache http requests?
CACHE_ENABLED = False

# spotify app client ID
CLIENT_ID = 'e021413b59f5430d9b1b0b46f67c9dec'

# spotify app client secret
CLIENT_SECRET = '1c155d57d1514944972ea4a6b7ed7554'

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
# REGIONS = ['th', 'ar', 'mt', 'fi', 'lv', 'be', 'hn', 'co', 'lu', 'ee', 'ni', 'dk', 'mc', 'ca', 'ad']

# max number of times to retry http requests
MAX_url_RETRIES = 10

# seconds to wait between retry attempts
SECONDS_BETWEEN_RETRIES = 3

# unverified SSL context
SSL_CONTEXT = ssl._create_unverified_context()

def get_page(url, count=0, last_request=0, return_full=False):
    """
    Request a webpage, retry on failure, cache as desired
    """
    if count > MAX_url_RETRIES:
        print('Failed getting page "{}", retried {} times'.format(url, count))
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

def get_dates_for_region_with_xpath(region):
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

def get_dates_for_region(region):
    # return value is an array of dates as stringsin YYYY-MM-DD format
    # eg. return ['2017-01-07', '2017-01-06', '2017-01-05', '2017-01-04', '2017-01-03', '2017-01-02', '2017-01-01']
    #
    try:
        url = 'https://spotifycharts.com/regional/{}/daily/'.format(region)
        r = get_page(url)
        soup = bs(r, 'html.parser')
        rows = [item['data-value'] for item in soup.find_all(attrs={"data-type": 'date'})[0].find_all('li', attrs={'data-value' : True})]
    except IndexError as e:
        return False
    else:
        # check that rows is valid
        if not isinstance(rows, list) and not len(rows):
            return False
    finally:
        return rows


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
        cache_file = "./cache/%s.cache" % hashedurl
        if cache and os.path.isfile(cache_file):
            with open(cache_file) as f:
                # return cached json
                data = f.read()
                return json.loads(data)
        if count > 3:
            # retried 3 times, giving up
            print('Failed getting page "%s", retried %i times' % (url, count))
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
            print('URLError = ' + str(e.reason))
            return get_page(url, cache, count, time.time())
        except ConnectionResetError as e:
            print('Error 54: Connection reset error. ', str(e))
            return get_page(url, cache, count, time.time())
        except HTTPError as err:
            if err.code == 400:
                print('HTTP 400, said:')
                # print(data)
            raise
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

def append_track_id_from_db(tracks):
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
    for track_id in tracks:
        query = """
            SELECT id
            FROM track
            WHERE service_id = 1
            AND service_track_id = %s
        """

        db.c.execute(query, [tracks[track_id]['trackId']])
        row = db.c.fetchone()
        if row:
            tracks[track_id]['track_id_db'] = row[0]

    # for key in tracks:
    #     logging.debug("TRACKS--------", tracks[key])

    return tracks

def append_track_data(tracks, batch_size=50):
    """
    Input:
        tracks: dict
    Output:
        tracks: dict +
            { 'isrc': xx, 'artistId': xx, 'albumId': xx} for any track without a db id
    Append the isrc, artist ID, and album ID to tracks_list using the Spotify tracks API
    See: https://developer.spotify.com/web-api/console/get-several-tracks/
    Returns track_list with "isrc", "artistID" and "albumId" appended
    """
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)
    endpoint = "https://api.spotify.com/v1/tracks?ids={}"

    tracks_to_lookup = []
    for track_id in tracks:
        # db id key would be appended to the track if it exists in the db
        if 'track_id_db' not in tracks[track_id]:
            tracks_to_lookup.append(track_id)
    # api supports up to 50 ids at a time
    batches = [tracks_to_lookup[i:i + batch_size] for i in range(0, len(tracks_to_lookup), batch_size)]
    for i, batch in enumerate(batches):
        id_str = ','.join(map(str, batch))
        r_dict = spotify.request(endpoint.format(id_str))
        for track_id in batch:
            tracks[track_id]['isrc'] = get_isrc_by_id(r_dict['tracks'], track_id)
            tracks[track_id]['albumId'] = get_album_by_id(r_dict['tracks'], track_id)
            tracks[track_id]['artistId'] = get_artist_by_id(r_dict['tracks'], track_id)
        print('Appended track data for batch %d of %d' % (i+1, len(batches)) )
    print('Added %i tracks to the DB' % len(tracks_to_lookup) )
    return tracks

def append_track_album_data(tracks, batch_size=20):
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
    albums = [t['albumId'] for k,t in tracks.items() if 'albumId' in t]

    if len(albums) != 1:
        batches = [albums[i:i + batch_size] for i in range(0, len(albums), batch_size)]
        for i, batch in enumerate(batches):
            id_str = ','.join(batch)
            r_dict = spotify.request(endpoint_albums.format(id_str))
            for album in r_dict['albums']:
                for track_id, track in tracks.items():
                    if 'albumId' in track and track['albumId'] == album['id']:
                        tracks[track_id]['release_date'] = album['release_date']
                        tracks[track_id]['label'] = album['label']
                        tracks[track_id]['album_name'] = album['name']
            print('Appended album data for batch %d of %d' % (i+1, len(batches)) )
    else:
        album_id = albums[0]
        album = spotify.request(endpoint_album.format(album_id))
        for track_id, track in tracks.items():
            if 'albumId' in track and track['albumId'] == album['id']:
                tracks[track_id]['release_date'] = album['release_date']
                tracks[track_id]['label'] = album['label']
                tracks[track_id]['album_name'] = album['name']
        print('Appended album data for album_id %s' % album_id )
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
    artists = [t['artistId'] for k,t in tracks.items() if 'artistId' in t]
    batches = [artists[i:i + batch_size] for i in range(0, len(artists), batch_size)]
    for i, batch in enumerate(batches):
        id_str = ','.join(batch)
        r_dict = spotify.request(endpoint.format(id_str))
        for artist in r_dict['artists']:
            for track_id, track in tracks.items():
                if 'artistId' in track and track['artistId'] == artist['id']:
                    tracks[track_id]['genres'] = artist['genres']
        print('Appended artist data for batch %d of %d' % (i+1, len(batches)) )
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
    """ PostgreSQLDatabase Manager """
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

    # default date to update is the previous day
    def update_regions(self, update_date = date.today() - timedelta(days = 1)):

        regions_processed = []
        start_url = 'https://spotifycharts.com/regional/'
        end_boundary_url = '/daily/'

        # get list of all processed urls for spotify streaming
        query = self.c.execute("""
            SELECT *
            FROM processed
            WHERE url
            LIKE '%{}%'
        """.format(update_date))
        rows = self.c.fetchall()

        if rows:
            for row in rows:
                try:
                    url = str(row[0])
                    region = url.split(start_url)[1].split(end_boundary_url)[0]
                    regions_processed.append(region)
                except IndexError as e:
                    pass

        regions = list(set(REGIONS).difference(regions_processed))
        # return all regions that haven't yet been processed for the given date
        return regions

    def is_processed(self, url):
        """
        Has CSV url already been processed?
        """
        query = "SELECT * FROM processed WHERE url = %s"
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

            self.db.commit()

        except psycopg2.IntegrityError as e:
            print('Integrity error: {}'.format(e))
        return True

    def get_track_stats(self, service_id, territory_id, track_id):
        """
        Returns a tuple of track stats
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

    def update_track_stats(self, service_id, territory_id, track_id, isrc, position, date_str):
        """
        Update the rolling stats for a track position and date
        """

        position = int(position)
        # latest track stats in the db
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

    def add_tracks(self, track_list, date_str, service_name):
        """
        input:
            track_list: dict of all songs to add
        Add tracks to the database
        """
        service_id = self.get_service_id(service_name)

        for track_id, track in track_list.items():

            territory_id = self.get_territory_id(track['region'])
            position = track['Position']

            # if db id is populated, the track is already in the DB
            if 'track_id_db' in track:
                track_id_db = track['track_id_db']
                isrc = self.get_isrc_from_db(track_id_db)

            # track doesn't have track.id and the data for the track and album were retrieved from Spotify API
            else:

                try:
                    # check if artist or album are in the db
                    artist_name = str(track['Artist'])
                    service_album_id = str(track['albumId'])
                    service_artist_id = str(track['artistId'])
                    isrc = str(track['isrc'])
                    artist_id = self.get_artist_id(service_id, service_artist_id)
                    album_id = self.get_album_id(service_id, service_album_id)


                    # add artist if not in the db
                    if not artist_id:
                        # add artist
                        self.c.execute("""
                            INSERT INTO artist
                            (service_id, service_artist_id, artist)
                            VALUES
                            (%s, %s, %s)
                            RETURNING id
                        """, (service_id, service_artist_id, artist_name))
                        artist_id = self.c.fetchone()[0]

                    # add album if not in the db
                    if not album_id:
                        self.c.execute("""
                            INSERT INTO album
                            (service_id, artist_id, service_album_id, album, release_date, label)
                            VALUES
                            (%s, %s, %s, %s, %s, %s)
                            RETURNING id
                            """,
                            (service_id, artist_id, service_album_id, track['album_name'], track['release_date'], track['label'])
                        )

                        album_id = self.c.fetchone()[0]
                        print('Album {} added: {} for {}'.format(album_id, track['album_name'], artist_name))

                    # add genres for artist
                    for genre in track['genres']:
                        self.c.execute("""
                            INSERT INTO artist_genre
                            (service_id, artist_id, genre)
                            VALUES
                            (%s, %s, %s)
                            """,
                            (service_id, artist_id, genre)
                        )

                    # update track table
                    #
                    self.c.execute("""
                        INSERT INTO track
                        (service_id, service_track_id, artist_id, album_id, track, isrc)
                        VALUES
                        (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (service_id, track_id, artist_id, album_id, track['Track Name'], isrc)
                    )

                    track_id_db = self.c.fetchone()[0]
                    print('Track {} added: {} by {}'.format(track_id_db, track['Track Name'], artist_name))

                except Exception as e:
                    print(e)
                    raise

            try:
                # update track_position_peak table
                self.update_track_stats(service_id, territory_id, track_id_db, isrc, position, date_str)

                # update track_position table
                self.c.execute("""
                    INSERT INTO track_position
                    (service_id, territory_id, track_id, isrc, position, stream_count, date_str)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (service_id, territory_id, track_id_db, isrc, position, track['Streams'], date_str)
                )
            except Exception as e:
                print(e)
                pass

        self.db.commit()


        return True

    def get_isrc_from_db(self, track_id):
        # RETRIEVE ISRC
        query = """
            SELECT isrc
            FROM track
            WHERE id = %s
        """

        self.c.execute(query, [track_id])
        row = self.c.fetchone()
        return row[0] if row else False

    def get_territory_id(self, code):
        """
        Retrieve territory_id from region code
        """
        query = self.c.execute("""
            SELECT id FROM territory WHERE code = %s
            """,
            [code.lower()]
        )

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
            """,
            (service_id, service_artist_id)
        )
        row = self.c.fetchone()
        return row[0] if row else False
    def get_album_id(self, service_id, service_album_id):
        """
        Retrive service_album_id
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

def process(mode = date.today() - timedelta(days = 1)):
    """
    Process each region for "date" mode
    Can be YYYY-MM-DD, "watch", "all", or "latest"
    """
    # TODO: could extract the loops, and while loop to
    # wrap the entire process(mode) function call in the AWS handler

    global db # for the try/except block to reset db if it for some reason doesn't exist

    starttime_total = datetime.now() # timestamping

    service_name = 'Spotify'

    loops = 0

    # set the initial set of regions to process
    regions = REGIONS

    while len(regions) > 0 and loops <= 3:
        try:
            for region in regions:
                # set dates to process
                if mode == 'latest':
                    # iterate through regions, get most recent date only
                    print('Getting most recent date available for region "%s"...' % region)
                    region_dates = get_dates_for_region(region)
                    if region_dates:
                        print('Most recent date is "%s".\n' % region_dates[0])
                        available_dates = [region_dates[0]]
                    else:
                        print('No date found for region "%s", skipping...' % region)
                        print('-' * 40)
                        continue
                else:
                    # get data for each region on literal user-supplied date string
                    available_dates = [mode]

                for date_to_process in available_dates:
                    starttime = datetime.now()
                    print('Starting processing at', starttime.strftime('%H:%M:%S %m-%d-%y'))
                    print('Loading tracks for region "%s" on "%s"...' % (region, date_to_process))
                    url = get_spotify_csv_url(region, date_to_process)
                    if db.is_processed(url):
                        print('Already processed, skipping...')
                        print('-' * 40)
                        continue
                    region_data = load_spotify_csv_data(region, date_to_process)

                    if not region_data:
                        print('No download available, skipping...')
                        print('-' * 40)
                        continue
                    print('Found %i tracks in the list.' % len(region_data))
                    print('Looking up tracks in database...')

                    # append data to Spotify API response
                    tracks = append_track_id_from_db(region_data)
                    print('Getting track data from Spotify "Tracks" API...')
                    tracks = append_track_data(region_data)
                    print('Getting label and release date from Spotify "Albums" API...')
                    tracks = append_track_album_data(tracks)
                    print('Getting genre tags from Spotify "Artists" API...')
                    tracks = append_artist_data(tracks)
                    print('Processed {} tracks, adding to database'.format(len(tracks)))
                    added = db.add_tracks(tracks, date_to_process, service_name)

                    # write processed url to DB, so it doesn't get run multiple times
                    db.set_processed(url)

                    # timestamp
                    endtime = datetime.now()
                    processtime = endtime - starttime
                    processtime_running_total = endtime - starttime_total
                    print('Finished processing at', endtime.strftime('%H:%M:%S %m-%d-%Y'))
                    print('Processing time: %i minutes, %i seconds' % divmod(processtime.days *86400 + processtime.seconds, 60))
                    print('Running processing time: %i minutes, %i seconds' % divmod(processtime_running_total.days *86400 + processtime_running_total.seconds, 60))
                    print('-' * 40)

            # update regions to process
            print('updating regions')
            regions = db.update_regions(date_to_process)

            # timestamping
            endtime_total = datetime.now()
            processtime_total = endtime_total - starttime_total
            print('Finished processing all applicable dates at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
            print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days *86400 + processtime_total.seconds, 60))
            print('-' * 40)

            # update loop count
            print('rerunning loop')
            loops += 1

        except psycopg2.InterfaceError as e:
            db = TrackDatabase()
            print('InterfaceError: ', e)
            print('rerunning loop')


# AWS LAMBDA HANDLER---
def handler(event, context):
    global db

    # set global db object
    db = TrackDatabase()

    # aws handler input
    mode = event['previous']
    process(mode)

    # clean up
    db.close_database()
    print('closed database connection')
    return 'finished'
