import sqlite3, csv, codecs, re, json, os, base64, time, hashlib, ssl, datetime
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError
from lxml import html

# cache http requests?
CACHE_ENABLED = False

# spotify app client ID
CLIENT_ID = 'e021413b59f5430d9b1b0b46f67c9dec'

# spotify app client secret
CLIENT_SECRET = '1c155d57d1514944972ea4a6b7ed7554'

# sqlite database filename/path
DATABASE_FILE = '../test-v4.db'

# the daily regional CSV download link
CSV_url = 'https://spotifycharts.com/regional/{}/daily/{}/download'

# the regions to download
# REGIONS = [
#     'global','gb','ad','ar','at','au','be','bg','bo','br',
#     'ca','ch','cl','co','cr','cy','cz','de','dk','do','ec',
#     'ee','es','fi','fr','gr','gt','hk','hn','hu','id','ie',
#     'is','it','jp','lt','lu','lv','mc','mt','mx','my','ni',
#     'nl','no','nz','pa','pe','ph','pl','pt','py','se','sg',
#     'sk','sv','th','tr','tw','uy'
# ]
# global only to test
REGIONS = ['global']

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
        print('Failed getting page "%s", retried %i times' % (url, count))
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
        # Add in csv field for territory 2-letter code, OR territoryId, or a dictionary lookup
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
            track['trackId'] = get_track_id_from_url(track['URL'])

            # set key value of dictionary to be spotify track id
            data[track['trackId']] = track
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
        except HTTPError as err:
            if err.code == 400:
                print('HTTP 400, said:')
                # print(data)
            raise
        except Exception as e:
            count += 1
            return get_page(url, cache, count, time.time())
#
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

def append_tracksId_from_db(tracks):
    """
    Input:
        tracks: dict
    Output:
        tracks: dict
    Appends key 'db_id' key with db lookup value.
    This removes redundancy from spotify API calls to retrieve info already in the DB.
    """
    for track_id in tracks:
        query = """
            SELECT
            tracks.id,
            spotify_trackId
            FROM track_service_info
            INNER JOIN tracks ON track_service_info.tracksId = tracks.id
            WHERE spotify_trackId = (?)
        """
        # NOTE: there should be a one-to-one relationship between spotify trackId and db_id
        row = db.c.execute(query, [tracks[track_id]['trackId']]).fetchone()
        if row:
            tracks[track_id]['db_id'] = row[0]

    # NOTE: Debug
    # for key in tracks:
    #     print("TRACKS--------", tracks[key])

    return tracks

def append_track_data(tracks, batch_size=50):
    """
    Input:
        tracks: dict
    Output:
        tracks: dict +
            { 'isrc': xx, 'artistId': xx, 'albumId': xx} for any track without a tracks.id
    Append the isrc, artist ID, and album ID to tracks_list using the Spotify tracks API
    See: https://developer.spotify.com/web-api/console/get-several-tracks/
    Returns track_list with "isrc", "artistID" and "albumId" appended
    """
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)
    endpoint = "https://api.spotify.com/v1/tracks?ids={}"

    tracks_to_lookup = []
    for track_id in tracks:
        # db_id key would be appended to the track if it exists in the db
        if 'db_id' not in tracks[track_id]:
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
            { 'released': xx, 'label': xx} for any track with 'albumId' key
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
                released = album['release_date']
                label = album['label']
                for track_id, track in tracks.items():
                    if 'albumId' in track and track['albumId'] == album['id']:
                        tracks[track_id]['released'] = released
                        tracks[track_id]['label'] = label
            print('Appended album data for batch %d of %d' % (i+1, len(batches)) )
    else:
        album_id = albums[0]
        album = spotify.request(endpoint_album.format(album_id))
        for track_id, track in tracks.items():
            if 'albumId' in track and track['albumId'] == album['id']:
                tracks[track_id]['released'] = album['release_date']
                tracks[track_id]['label'] = album['label']
        print('Appended album data for album_id %s' % album_id )
    return tracks

def append_artist_data(tracks, batch_size=50):
    """
    Append the genre tags to tracks_list using the Spotify artists API
    See: https://developer.spotify.com/web-api/console/get-several-artists/
    Returns track_list with "Genre" appended
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
                    tracks[track_id]['genres'] = ','.join(artist['genres'])
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
    """ SQLite Database Manager """
    def __init__(self, db_file='test-v4.db'):
        super(TrackDatabase, self).__init__()
        self.db_file = db_file
        self.init_database()
    def init_database(self):
        print('Initializing database...')
        self.db = sqlite3.connect(self.db_file)
        self.c = self.db.cursor()

        #tracks table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS tracks (
                id integer PRIMARY KEY AUTOINCREMENT,
                track_name text NOT NULL,
                artist text NOT NULL,
                label text NOT NULL,
                isrc varchar(255) NOT NULL,
                release_date varchar(255) NOT NULL,
                genres varchar(255) NULL
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
                serviceId integer PRIMARY KEY,
                service_name varchar(255) NOT NULL
            )
        ''')

        # seed service table
        self.c.execute('''
            INSERT OR IGNORE INTO service
            (serviceId, service_name)
            VALUES
            (?, ?)
        ''', (1111, 'Spotify')
        )

        # stats table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                id integer PRIMARY KEY AUTOINCREMENT,
                tracksId integer NOT NULL,
                territoryId integer NOT NULL,
                serviceId integer NOT NULL,
                added varchar(255) NOT NULL,
                last_seen varchar(255) NOT NULL,
                peak_rank integer NOT NULL,
                peak_date varchar(255) NOT NULL
            )
        ''')

        # track_position table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS track_position (
                id integer PRIMARY KEY AUTOINCREMENT,
                tracksId integer NOT NULL,
                territoryId integer NOT NULL,
                serviceId integer NOT NULL,
                position integer NOT NULL,
                streams integer NOT NULL DEFAULT 0,
                date_str varchar(255) NOT NULL
            )
        ''')

        # territory table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS territory (
                territoryId integer PRIMARY KEY AUTOINCREMENT,
                code varchar(10) NOT NULL,
                name varchar(255) NOT NULL
            )
        ''')

        self.c.execute('''
            INSERT OR IGNORE INTO territory
            (code, name)
            VALUES
            (?, ?)
        ''', ('global', 'global')
        )

        # track_service_info table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS track_service_info (
                tracksId integer NOT NULL PRIMARY KEY,
                spotify_url text NOT NULL,
                spotify_trackId varchar(255) NOT NULL,
                spotify_albumId varchar(255) NOT NULL
            )
        ''')

        stat = os.stat(self.db_file)
        print("Using Database '%s'" % self.db_file)
        print("# Bytes: %r" % stat.st_size)
        bq = self.c.execute("SELECT COUNT(*) FROM processed")
        print("# urls Processed: %r" % bq.fetchone()[0])
        tq = self.c.execute("SELECT COUNT(*) FROM tracks")
        print("# Tracks: %r" % tq.fetchone()[0])
        sq = self.c.execute("SELECT COUNT(*) FROM stats")
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
    def get_track_stats(self, tracksId):
        """
        Returns a tuple of track stats (tracksId, territoryId, serviceId, added, last_seen, peak_rank, peak_date)
        """
        query = self.c.execute('''
            SELECT * FROM stats WHERE tracksId = ?
        ''', [tracksId])
        return query.fetchone() if query else False
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
    def update_track_stats(self, tracksId, territoryId, serviceId, position, date_str):
        """
        Update the rolling stats for a track
        """
        position = int(position)
        # latest track stats in the db
        stats = self.get_track_stats(tracksId)
        # destructure to readable variables
        if stats:
            tracksId, territoryId, serviceId, added, last_seen, peak_rank, peak_date = stats
        stats_query = '''
            INSERT OR REPLACE INTO stats
            (tracksId, territoryId, serviceId, added, last_seen, peak_rank, peak_date)
            VALUES
            (?, ?, ?, ?, ?, ?, ?)
        '''
        # finds the earlier of the two dates, the current added and the current date query
        # this is important because of the asynchronous nature of collecting the data - the script can download
        # any date from spotify at any time, it is not necessarily in chronological order of when the data is processed
        added = self.order_dates(added, date_str)[0] if stats else date_str
        # finds the later of the current last_seen and the current date query
        last_seen = self.order_dates(last_seen, date_str)[1] if stats else date_str
        if stats and position < peak_rank:
            # track is ranked higher (has a lower numbered position) when current position is less than old now
            peak_rank = position
            peak_date = date_str
        else:
            # track was higher then, or doesn't have stats
            peak_rank = peak_rank if stats else position
            peak_date = self.order_dates(peak_date, date_str) if stats else date_str # use the earliest peak date for the peak rank
        self.c.execute(
            stats_query,
            [tracksId, territoryId, serviceId, added, last_seen, peak_rank, peak_date]
        )
    def add_tracks(self, track_list, date_str, service_name):
        """
        input:
            track_list: dict of all songs to add
        Add tracks to the database
        """
        serviceId = self.get_serviceId(service_name)

        for track_id, track in track_list.items():

            territoryId = self.get_territoryId(track['region'])
            position = track['Position']

            # if db_id is populated, the track is already in the DB
            if 'db_id' in track:
                db_id = track['db_id']

            # track doesn't have tracks.id and the data for the track and album were retrieved from Spotify API
            else:
                row = self.track_to_tuple(track)

                try:
                    # update tracks table
                    # add the new track
                    self.c.execute('''
                        INSERT OR IGNORE INTO tracks
                        (track_name, artist, label,
                         isrc, release_date, genres)
                        VALUES
                        (?, ?, ?, ?, ?, ?)
                    ''', row)

                    db_id = self.c.execute('''
                        SELECT LAST_INSERT_ROWID()
                    ''').fetchone()[0]

                    print('THE NEWLY INSERTED ROW IS: {}'.format(db_id))
                    # update track_service_info table
                    self.c.execute('''
                        INSERT OR IGNORE INTO track_service_info
                        (tracksId, spotify_url, spotify_trackId, spotify_albumId)
                        VALUES
                        (?, ?, ?, ?)
                    ''', [db_id, track['URL'], track['trackId'], track['albumId']])

                except Exception as e:
                    print(e)
                    raise

            # update stats table
            self.update_track_stats(db_id, territoryId, serviceId, position, date_str)

            # update track_position table
            self.c.execute('''
                INSERT OR IGNORE INTO track_position
                (tracksId, territoryId, serviceId, position, streams, date_str)
                VALUES
                (?, ?, ?, ?, ?, ?)
            ''', [db_id, territoryId, serviceId, position, track['Streams'], date_str]
            )

            self.db.commit()


        return True

    def track_to_tuple(self, track):
        """
        Convert a track dict into a tuple
        """
        return (
            str(track['Track Name']),
            str(track['Artist']),
            str(track['label']),
            str(track['isrc']),
            str(track['released']),
            str(track['genres'])
        )
    def get_territoryId(self, code):
        """
        Retrieve territoryId from region code
        """
        code = code.lower()
        query = self.c.execute('''
            SELECT territoryId FROM territory WHERE code = ?
        ''', [code])
        territoryId = query.fetchone()[0]
        return territoryId

    def get_serviceId(self, service_name):
        """
        Retrieve serviceId from service name
        """
        query = self.c.execute('''
            SELECT serviceId FROM service WHERE service_name = ?
        ''', [service_name])
        return query.fetchone()[0] if query else false

def process(mode):
    """
    Process each region for "date" mode
    Can be YYYY-MM-DD, "watch", "all", or "latest"
    """
    starttime_total = datetime.datetime.now() # timestamping

    service_name = 'Spotify'
    for region in REGIONS:
        if mode == 'all':
            # gets all historical data for each region
            print('Getting all dates available for region "%s"...' % region)
            region_dates = get_dates_for_region(region)
            if region_dates:
                print('Found %i dates.\n' % len(region_dates))
                available_dates = region_dates
            else:
                print('No dates found for region "%s", skipping...' % region)
                print('-' * 40)
                continue
        elif mode == 'watch' or mode == 'latest':
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

        for date_str in available_dates:
            starttime = datetime.datetime.now()
            print('Starting processing at', starttime.strftime('%H:%M:%S %m-%d-%y'))
            print('Loading tracks for region "%s" on "%s"...' % (region, date_str))
            url = get_spotify_csv_url(region, date_str)
            if db.is_processed(url):
                print('Already processed, skipping...')
                print('-' * 40)
                continue
            region_data = load_spotify_csv_data(region, date_str)
            # NOTE: DEBUG:
            # print(region_data)
            if not region_data:
                print('No download available, skipping...')
                print('-' * 40)
                continue
            print('Found %i tracks.' % len(region_data))
            print('Looking up tracks in database...')

            # append data to Spotify API response
            tracks = append_tracksId_from_db(region_data)
            print('Getting track data from Spotify "Tracks" API...')
            tracks = append_track_data(region_data)
            print('Getting label and release date from Spotify "Albums" API...')
            tracks = append_track_album_data(tracks)
            print('Getting genre tags from Spotify "Artists" API...')
            tracks = append_artist_data(tracks)
            print('Processed %i tracks, adding to database' % len(tracks))
            added = db.add_tracks(tracks, date_str, service_name)

            # write data to DB
            db.set_processed(url)

            # timestamp
            endtime = datetime.datetime.now()
            processtime = endtime - starttime
            processtime_running_total = endtime - starttime_total
            print('Finished processing at', endtime.strftime('%H:%M:%S %m-%d-%y'))
            print('Processing time: %i minutes, %i seconds' % divmod(processtime.days *86400 + processtime.seconds, 60))
            print('Running processing time: %i minutes, %i seconds' % divmod(processtime_running_total.days *86400 + processtime_running_total.seconds, 60))
            print('-' * 40)

    # timestamping
    endtime_total = datetime.datetime.now()
    processtime_total = endtime_total - starttime_total
    print('Finished processing all applicable dates at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
    print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days *86400 + processtime_total.seconds, 60))
    print('-' * 40)
if __name__ == '__main__':

    # are http requests being cached?
    #CACHE_ENABLED = True
    cache_msg = '\033[92m enabled' if CACHE_ENABLED else '\033[91m disabled'
    print('HTTP cache is%s\033[0m' % cache_msg)

    # setup db
    db = TrackDatabase(DATABASE_FILE)

    # prompt for date/mode
    while True:
        mode = input('\nEnter a date (YYYY-MM-DD) or use "all|watch|latest": ')
        mode = mode.lower()
        if re.match(r'\d{4}-\d{2}-\d{2}|all|watch|latest', mode):
            break
        else:
            print('Invalid date, try again.')

    print('-' * 40)
    print()

    while True:
        process(mode)
        if mode == 'watch':
            print()
            print('=' * 40)
            print('\033[95mWatch cycle complete for all regions, starting over...\033[0m')
            print('=' * 40)
            print()
        else:
            break
    print('Finished')
