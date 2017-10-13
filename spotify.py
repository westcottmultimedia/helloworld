import sqlite3, csv, codecs, re, json, os, base64, time, hashlib
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from lxml import html

# cache http requests?
CACHE_ENABLED = False

# spotify app client ID
CLIENT_ID = 'e021413b59f5430d9b1b0b46f67c9dec'

# spotify app client secret
CLIENT_SECRET = '1c155d57d1514944972ea4a6b7ed7554'

# sqlite database filename/path
DATABASE_FILE = '../Spotify-SQLite.db'

# the daily regional CSV download link
CSV_URL = 'https://spotifycharts.com/regional/{}/daily/{}/download'

# the regions to downloada
REGIONS = [
    'global','gb','ad','ar','at','au','be','bg','bo','br',
    'ca','ch','cl','co','cr','cy','cz','de','dk','do','ec',
    'ee','es','fi','fr','gr','gt','hk','hn','hu','id','ie',
    'is','it','jp','lt','lu','lv','mc','mt','mx','my','ni',
    'nl','no','nz','pa','pe','ph','pl','pt','py','se','sg',
    'sk','sv','th','tr','tw','uy'
]

# max number of times to retry http requests
MAX_URL_RETRIES = 10

# seconds to wait between retry attempts
SECONDS_BETWEEN_RETRIES = 3

def get_page(url, cache=False, count=0, last_request=0, return_full=False):
    """
    Request a webpage, retry on failure, cache as desired
    """
    if cache and return_full:
        raise ValueError('Cannot have "cache" set to True while "return_full" is set to True')
    hashedurl = hashlib.sha256(url.encode('utf-8')).hexdigest()
    cache_file = "./cache/%s.cache" % hashedurl
    if cache and os.path.isfile(cache_file):
        with open(cache_file) as f:
            return f.read()
    if count > MAX_URL_RETRIES:
        print('Failed getting page "%s", retried %i times' % (url, count))
        return False
    if last_request > time.time()-1:
        time.sleep(SECONDS_BETWEEN_RETRIES)
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive'
    }
    if cache and not os.path.exists(os.path.dirname(cache_file)):
        try:
            os.makedirs(os.path.dirname(cache_file))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
    try:
        q = Request(url, None, headers)
        r = urlopen(q)
        data = r.read().decode('utf-8')
        if cache:
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(data)
        return r if return_full else data
    except Exception as e:
        count += 1
        print('Failed getting URL "%s", retrying...' % url)
        return get_page(url, cache, count, time.time())

def get_dates_for_region(region):
    """
    Scrape the available chart dates for a given region
    Returns a list of date strings
    """
    url = 'https://spotifycharts.com/regional/{}/daily/'.format(region)
    r = get_page(url)
    page = html.fromstring(r)
    xpath = '*//div[contains(concat(" ", normalize-space(@data-type), " "), " date ")]/ul/li/text()'
    rows = page.xpath(xpath)
    if not isinstance(rows, list) and not len(rows):
        return False
    # convert M/D/Y to Y-M-D
    return [re.sub(r"(\d{2})\/(\d{2})\/(\d{4})", '\\3-\\1-\\2', d, 0) for d in rows]

def get_csv_url(region, date='latest'):
    return CSV_URL.format(region, date)

def load_csv_data(region, date='latest'):
    """
    Load and process the CSV file for a given region and date
    Returns a list of tracks with region and track ID appended
    """
    url = get_csv_url(region, date)
    r = get_page(url, return_full=True)
    info = r.info()
    if info.get_content_type() != 'text/csv':
        return False
    rows = csv.reader(codecs.iterdecode(r, 'utf-8'))
    fields = None
    data = {}
    for row in rows:
        if not fields:
            fields = row
            continue
        track = dict(zip(fields, row))
        if len(track) == len(fields):
            track['Region'] = region
            track['TrackID'] = get_track_id(track['URL']) 
            data[track['TrackID']] = track
    return data

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
        response = urlopen(r).read()
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
            data = urlopen(q).read().decode('utf-8')
            if cache:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    f.write(data)
            return json.loads(data)
        except urllib2.HTTPError as err:
            if err.code == 400:
                print('HTTP 400, said:')
                print(data)
            raise
        except Exception as e:
            count += 1
            return get_page(url, cache, count, time.time())

def get_isrc_by_id(tracks, track_id):
    """
    Return the ISRC data for the track matching track_id
    """
    for track in tracks:
        if track['id'] == track_id:
            if 'external_ids' in track and 'isrc' in track['external_ids']:
                return track['external_ids']['isrc']
            else:
                print('ISRC data not available for track ID %s' % track_id)
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
                print('Artist ID not available for track ID %s' % track_id)
    return False

def append_track_data(tracks, batch_size=50):
    """
    Append the ISRC, artist ID, and album ID to tracks_list using the Spotify tracks API
    See: https://developer.spotify.com/web-api/console/get-several-tracks/
    Returns track_list with "ISRC", "ArtistID" and "AlbumID" appended
    """
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)
    endpoint = "https://api.spotify.com/v1/tracks?ids={}"
    # api supports up to 50 ids at a time
    track_list = list(tracks)
    batches = [track_list[i:i + batch_size] for i in range(0, len(track_list), batch_size)]
    for batch in batches:
        id_str = ','.join(map(str, batch))
        r_dict = spotify.request(endpoint.format(id_str))
        for track_id in batch:
            tracks[track_id]['ISRC'] = get_isrc_by_id(r_dict['tracks'], track_id)
            tracks[track_id]['AlbumID'] = get_album_by_id(r_dict['tracks'], track_id)
            tracks[track_id]['ArtistID'] = get_artist_by_id(r_dict['tracks'], track_id)
    return tracks

def append_track_album_data(tracks, batch_size=20):
    """
    Append the label and release date to tracks using the Spotify albums API
    See: hhttps://developer.spotify.com/web-api/console/get-several-albums/
    Returns tracks with "Label" and "Released" appended
    """
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)
    endpoint = "https://api.spotify.com/v1/albums?ids={}"
    # api supports up to 20 ids at a time
    albums = [t['AlbumID'] for k,t in tracks.items() if t['AlbumID']]
    batches = [albums[i:i + batch_size] for i in range(0, len(albums), batch_size)]
    for batch in batches:
        id_str = ','.join(batch)
        r_dict = spotify.request(endpoint.format(id_str))
        for album in r_dict['albums']:
            released = album['release_date']
            label = album['label']
            for track_id, track in tracks.items():
                if track['AlbumID'] == album['id']:
                    tracks[track_id]['Released'] = released
                    tracks[track_id]['Label'] = label
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
    artists = [t['ArtistID'] for k,t in tracks.items() if t['ArtistID']]
    batches = [artists[i:i + batch_size] for i in range(0, len(artists), batch_size)]
    for batch in batches:
        id_str = ','.join(batch)
        r_dict = spotify.request(endpoint.format(id_str))
        for artist in r_dict['artists']:
            for track_id, track in tracks.items():
                if track['ArtistID'] == artist['id']:
                    tracks[track_id]['Genres'] = ','.join(artist['genres'])
    return tracks

def get_track_id(url):
    """
    Return the Spotify track ID from a given URL
    Example: https://open.spotify.com/track/r1OmcAT5Y8UPv9qJT4R
    """
    regex = r"open\.spotify\.com\/track\/(\w+)"
    matches = re.search(regex, url)
    assert matches, "No track ID found for {}".format(url)
    return matches.group(1)

class TrackDatabase(object):
    """ SQLite Database Manager """
    def __init__(self, db_file='Spotify-SQLite.db'):
        super(TrackDatabase, self).__init__()
        self.db_file = db_file
        self.init_database()
    def init_database(self):
        print('Initializing database...')
        self.db = sqlite3.connect(self.db_file)
        self.c = self.db.cursor()
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS tracks (
                hash text PRIMARY KEY,
                Name text NOT NULL,
                Artist text NOT NULL,
                Label text NOT NULL,
                TrackID varchar(255) NOT NULL,
                AlbumID varchar(255) NOT NULL,
                URL text NOT NULL,
                Region varchar(10) NOT NULL,
                ISRC varchar(255) NOT NULL,
                Released varchar(255) NOT NULL,
                Genres varchar(255) NULL
            )
        ''')
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS processed (
                url text PRIMARY KEY
            )
        ''')
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                track_hash text PRIMARY KEY,
                added varchar(255) NOT NULL,
                last_seen varchar(255) NOT NULL,
                peak_rank integer NOT NULL,
                peak_date varchar(255) NOT NULL
            )
        ''')
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS track_position (
                hash varchar(255) PRIMARY KEY,
                track_hash varchar(255) NOT NULL,
                position integer NOT NULL,
                streams integer NOT NULL,
                date_str varchar(255) NOT NULL
            )
        ''')
        stat = os.stat(self.db_file)
        print("Using Database '%s'" % self.db_file)
        print("# Bytes: %r" % stat.st_size)
        bq = self.c.execute("SELECT COUNT(*) FROM processed")
        print("# URLs Processed: %r" % bq.fetchone()[0])
        tq = self.c.execute("SELECT COUNT(*) FROM tracks")
        print("# Tracks: %r" % tq.fetchone()[0])
        sq = self.c.execute("SELECT COUNT(*) FROM stats")
        print("# Track Stats: %r" % sq.fetchone()[0])
        pq = self.c.execute("SELECT COUNT(*) FROM track_position")
        print("# Position Stats: %r\n" % pq.fetchone()[0])
    def is_processed(self, url):
        """
        Has CSV URL already been processed?
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
        except Exception as e:
            raise e
        return True
    def get_track_stats(self, track_hash):
        """
        Returns a tuple of track stats (hash, added, last, peak, peak date)
        """
        query = self.c.execute('''
            SELECT * FROM stats WHERE track_hash = ?
        ''', [track_hash])
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
    def update_track_stats(self, track_hash, position, date_str):
        """
        Update the rolling stats for a track
        """
        position = int(position)
        stats = self.get_track_stats(track_hash)
        stats_query = '''
            INSERT OR REPLACE INTO stats 
            (track_hash, added, last_seen, peak_rank, peak_date) 
            VALUES 
            (?, ?, ?, ?, ?)
        '''
        added = self.order_dates(stats[1], date_str)[0] if stats else date_str
        if stats and stats[3]<position:
            # track is higher now
            peak = position
            peak_date = date_str
        else:
            # track was higher then, or doesn't have stats
            peak = stats[3] if stats else position
            peak_date = stats[2] if stats else date_str
        self.c.execute(
            stats_query, 
            [track_hash, added, date_str, peak, peak_date]
        )
    def add_tracks(self, track_list, date_str):
        """
        Add tracks to the database
        """
        for track_id, track in track_list.items():
            row = self.track_to_tuple(track)
            try:
                self.c.execute('''
                    INSERT OR IGNORE INTO tracks 
                    (hash, Name, Artist, Label, TrackID, AlbumID,
                     URL, Region, ISRC, Released, Genres)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)
                self.update_track_stats(row[0], track['Position'], date_str)
                pos_hash = self.get_track_position_hash(row[0], date_str)
                self.c.execute('''
                    INSERT OR IGNORE INTO track_position 
                    (hash, track_hash, position, streams, date_str)
                    VALUES
                    (?, ?, ?, ?, ?)
                ''', [pos_hash, row[0], track['Position'], track['Streams'], date_str])
                self.db.commit()
            except Exception as e:
                print(e)
                raise
        return True
    def track_to_tuple(self, track):
        """
        Convert a track dict into a tuple
        """
        hashed = self.get_row_hash(track)
        return (
            hashed, 
            str(track['Track Name']),
            str(track['Artist']),
            str(track['Label']),
            str(track['TrackID']),
            str(track['AlbumID']),
            str(track['URL']),
            str(track['Region']),
            str(track['ISRC']),
            str(track['Released']),
            str(track['Genres'])
        )
    def get_row_hash(self, track):
        """
        Return an SHA1 hash for a given track 
        """
        hash_str = "%r-%r-%r-%r" % (track['ISRC'],track['Region'],track['TrackID'],track['AlbumID'])
        return hashlib.sha1(hash_str.encode('utf-8')).hexdigest()
    def get_track_position_hash(self, track_hash, date_str):
        """
        Return an SHA1 hash for a given track and position 
        """
        hash_str = "%r:%r" % (track_hash,date_str)
        return hashlib.sha1(hash_str.encode('utf-8')).hexdigest()


def process(mode):
    """
    Process each region for "date" mode
    Can be YYYY-MM-DD, "watch", "all", or "latest"
    """
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
            print('Loading tracks for region "%s" on "%s"...' % (region, date_str))
            url = get_csv_url(region, date_str)
            if db.is_processed(url):
                print('Already processed, skipping...')
                print('-' * 40)
                continue
            region_data = load_csv_data(region, date_str)
            if not region_data:
                print('No download available, skipping...')
                print('-' * 40)
                continue
            print('Found %i tracks.' % len(region_data))
            print('Getting track data from Spotify "tracks" API...')
            tracks = append_track_data(region_data)
            print('Getting label and release date from Spotify "albums" API...')
            tracks = append_track_album_data(tracks)
            print('Getting genre tags from Spotify "artists" API...')
            tracks = append_artist_data(tracks)
            print('Processed %i tracks, adding to database' % len(tracks))
            added = db.add_tracks(tracks, date_str)
            db.set_processed(url)
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