import sqlite3, csv, codecs, re, json, os, base64, time, hashlib, ssl, datetime, datetime, jwt, configparser
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError
from lxml import html
from pprint import pprint
from datetime import date

'''BLUEPRINT
0. Find data accuracy of API chart data and RSS feed generator
    -close but not 100% the same. The songs are generally the same, but positions can be slightly different.
    -RSS feed json has timestamp of updated time. API feed does not.
1. download day's RSS feed generator for apple music.
    -for each country code
1a. download all 200 songs...
2. Save json data into folder.
    -figure out naming/folder structure
2a. For later project scope, download apple music videos, etc.for later seeding the database
2. parse top lists for top 200
3. per song, make api calls for album data and/or any other data needed
    -setup apple class to make API calls
4. get response, parse into data needed for db input
5. test

'''

# cache http requests?
CACHE_ENABLED = False

# sqlite database filename/path
DATABASE_FILE = '../test.db'

# the Apple API url
# ie. https://api.music.apple.com/v1/catalog/{storefront}/genres/{id}
API_url = 'https://api.music.apple.com/v1/catalog/{}/{}?limit=50&types={}'
# https://api.music.apple.com/v1/catalog/us/charts?types=songs&limit=50

# Apple RSS Feed url
# ie. https://rss.itunes.apple.com/api/v1/us/apple-music/hot-tracks/all/100/explicit.json
RSS_url = 'https://rss.itunes.apple.com/api/v1/{region}/{media}/{chart}/{genre}/{limit}/explicit.json'


# the regions to download
# REGIONS = ["us", "gb", "vn", "mn", "za", "mz", "mr", "tw", "fm", "sg", "gw", "cn", "kg", "jp", "fj",
#     "hk", "gm", "mx", "co", "mw", "ru", "ve", "kr", "la", "in", "lr", "ar", "sv", "br",
#     "gt", "ec", "pe", "do", "hu", "cl", "tr", "ae", "th", "id", "pg", "my", "na", "ph",
#     "pw", "sa", "ni", "py", "pk", "hn", "st", "pl", "jm", "sc", "eg", "kz", "uy", "mo",
#     "ee", "lv", "kw", "hr", "il", "ua", "lk", "ro", "lt", "np", "pa", "md", "am", "mt", "cz",
#     "jo", "bw", "bg", "ke", "lb", "mk", "qa", "mg", "cr", "sk", "ne", "sn", "si", "ml", "mu",
#     "ai", "bs", "tn", "ug", "bb", "bm", "ag", "dm", "gd", "vg", "ky", "lc", "ms", "kn", "bn",
#     "tc", "gy", "vc", "tt", "bo", "cy", "sr", "bz", "is", "bh", "it", "ye", "fr", "dz", "de",
#     "ao", "ng", "om", "be", "sl", "fi", "az", "sb", "by", "at", "uz", "tm", "zw",
#     "gr", "sz", "ie", "tj", "au", "td", "nz", "cg", "cv", "pt", "es", "al", "lu", "tz", "nl",
#     "gh", "no", "bf", "dk", "kh", "ca", "bj", "se", "bt", "ch"]

# global only to test
REGIONS = ['us']

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
        config_file = 'apple_api.config'
        config_section = 'APPLE_API'
        config = configparser.ConfigParser()
        config.read(config_file)
        if config_section in config:
            # newlines are essential to correct functioning, and the only way to create it properly is thru this script
            self.secret = config[config_section]['SECRET0'] + "\n" + config[config_section]['SECRET1'] + "\n" + config[config_section]['SECRET2']+ "\n" + config[config_section]['SECRET3'] + "\n" + config[config_section]['SECRET4'] +"\n" + config[config_section]['SECRET5']
            self.keyId = config[config_section]['KEYID']
            self.teamId = config[config_section]['TEAMID']
            alg = 'ES256'
            headers = {
            	"typ": 'JWT',
            	"alg": alg,
            	"kid": self.keyId
            }
            time_now = datetime.datetime.now()
            time_expired = datetime.datetime.now() + datetime.timedelta(hours=12)
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

        if count > 3:
            # retried 3 times, giving up
            print('Failed getting page "%s", retried %i times' % (url, count))
            return False
        if last_request > time.time()-1:
            # wait 3 seconds between retries
            time.sleep(3)
        try:
            q = Request(url)
            q.add_header('Authorization', 'Bearer {}'.format(self.token))
            data = urlopen(q).read().decode('utf8')
            response = json.loads(data)

            # Debug: did you receive anything?
            pprint(response, depth=2)

            # write data to file
            file = open(cache_file, 'w+')
            file.write(data)
            # print('written!')

            # pprint(response['results']['songs'][0]['data'][0]['attributes'], depth=2)
            # test
            songs = response['results']['songs'][0]['data']
            for song in songs:
                print(song['attributes']['name'])

            return response
        except HTTPError as err:
            if err.code == 400:
                print('HTTP 400, said:')

            # raise
        except Exception as e:
            count += 1
            print('Exception in API request, retrying...')
            # return request(url, cache, count, time.time())
#
#
# APPLE CLASS END

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

def append_track_hash_from_db(tracks):
    """
    Input:
        tracks: dict
    Output:
        tracks: dict
    Appends key 'track_hash' key with db lookup value.
    This removes redundancy from spotify API calls to retrieve info already in the DB.
    """
    for track_id in tracks:
        query = """
            SELECT
            tracks.track_hash,
            spotify_trackId
            FROM track_service_info
            INNER JOIN tracks ON track_service_info.track_hash = tracks.track_hash
            WHERE spotify_trackId = (?)
        """
        # NOTE: may need to fetch more than one, for example, "Trinidad"
        row = db.c.execute(query, [tracks[track_id]['trackId']]).fetchone()
        if row:
            tracks[track_id]['track_hash'] = row[0]

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
            { 'isrc': xx, 'artistId': xx, 'albumId': xx} for any track without a 'track_hash'
    Append the isrc, artist ID, and album ID to tracks_list using the Spotify tracks API
    See: https://developer.spotify.com/web-api/console/get-several-tracks/
    Returns track_list with "isrc", "artistID" and "albumId" appended
    """
    spotify = Spotify(CLIENT_ID, CLIENT_SECRET)
    endpoint = "https://api.spotify.com/v1/tracks?ids={}"

    tracks_to_lookup = []
    for track_id in tracks:
        # 'track_hash' key would be appended to the track if it exists in the db
        if 'track_hash' not in tracks[track_id]:
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

# TrackDatabase class start
#
#
class TrackDatabase(object):
    """ SQLite Database Manager """
    def __init__(self, db_file='test.db'):
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
                track_hash text PRIMARY KEY,
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
                track_hash text PRIMARY KEY,
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
                hash varchar(255) PRIMARY KEY,
                track_hash varchar(255) NOT NULL,
                territoryId integer NOT NULL,
                serviceId integer NOT NULL,
                position integer NOT NULL,
                streams integer NOT NULL,
                date_str varchar(255) NOT NULL
            )
        ''')

        # territory table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS territory (
                territoryId integer PRIMARY KEY,
                code varchar(10) NOT NULL,
                name varchar(255) NOT NULL
            )
        ''')

        self.c.execute('''
            INSERT OR IGNORE INTO territory
            (territoryId, code, name)
            VALUES
            (?, ?, ?)
        ''', (1, 'global', 'global')
        )

        # track_service_info table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS track_service_info (
                track_hash varchar(255) NOT NULL PRIMARY KEY,
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
    def get_track_stats(self, track_hash):
        """
        Returns a tuple of track stats (track_hash, territoryId, serviceId, added, last_seen, peak_rank, peak_date)
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
    def update_track_stats(self, track_hash, territoryId, serviceId, position, date_str):
        """
        Update the rolling stats for a track
        """
        position = int(position)
        # latest track stats in the db
        stats = self.get_track_stats(track_hash)
        # destructure to readable variables
        if stats:
            track_hash, territoryId, serviceId, added, last_seen, peak_rank, peak_date = stats
        stats_query = '''
            INSERT OR REPLACE INTO stats
            (track_hash, territoryId, serviceId, added, last_seen, peak_rank, peak_date)
            VALUES
            (?, ?, ?, ?, ?, ?, ?)
        '''
        # finds the earlier of the two dates, the current added and the current date query
        added = self.order_dates(added, date_str)[0] if stats else date_str
        # finds the later of the current last_seen and the current date query
        last_seen = self.order_dates(last_seen, date_str)[1] if stats else date_str
        if stats and position < peak_rank:
            # track is ranked higher when current position is less than old now
            peak_rank = position
            peak_date = date_str
        else:
            # track was higher then, or doesn't have stats
            peak_rank = peak_rank if stats else position
            peak_date = peak_date if stats else date_str
        self.c.execute(
            stats_query,
            [track_hash, territoryId, serviceId, added, last_seen, peak_rank, peak_date]
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

            # track_hash (hash from the DB) is in the track dict
            # thus, the track is already in the DB
            if 'track_hash' in track:
                track_hash = track['track_hash']
                pos_hash = self.get_track_position_hash(track_hash, date_str, territoryId, serviceId)
            # track doesn't have track_hash and the data for the track and album were retrieved from Spotify API

            else:
                row = self.track_to_tuple(track)
                track_hash = row[0]
                pos_hash = self.get_track_position_hash(track_hash, date_str, territoryId, serviceId)

                try:
                    # update tracks table
                    # add the new track
                    self.c.execute('''
                        INSERT OR IGNORE INTO tracks
                        (track_hash, track_name, artist, label,
                         isrc, release_date, genres)
                        VALUES
                        (?, ?, ?, ?, ?, ?, ?)
                    ''', row)

                    # update track_service_info table
                    self.c.execute('''
                        INSERT OR IGNORE INTO track_service_info
                        (track_hash, spotify_url, spotify_trackId, spotify_albumId)
                        VALUES
                        (?, ?, ?, ?)
                    ''', [track_hash, track['URL'], track['trackId'], track['albumId']])

                except Exception as e:
                    print(e)
                    raise

            # update stats table
            self.update_track_stats(track_hash, territoryId, serviceId, position, date_str)

            # update track_position table
            self.c.execute('''
                INSERT OR IGNORE INTO track_position
                (hash, track_hash, territoryId, serviceId, position, streams, date_str)
                VALUES
                (?, ?, ?, ?, ?, ?, ?)
            ''', [pos_hash, track_hash, territoryId, serviceId, position, track['Streams'], date_str]
            )

            self.db.commit()


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
            str(track['label']),
            str(track['isrc']),
            str(track['released']),
            str(track['genres'])
        )
    def get_row_hash(self, track):
        """
        Return an SHA1 hash for a given track
        """
        hash_str = "%r-%r-%r-%r" % (track['isrc'],track['region'],track['trackId'],track['albumId'])
        return hashlib.sha1(hash_str.encode('utf-8')).hexdigest()
    def get_track_position_hash(self, track_hash, date_str, territoryId, serviceId):
        """
        Return an SHA1 hash for a given track and position
        """
        hash_str = "%r:%r:%r:%r" % (track_hash,date_str,territoryId,serviceId)
        return hashlib.sha1(hash_str.encode('utf-8')).hexdigest()
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
    Process each region
    """

    number_results = 50
    limit = '&limit={}'.format(number_results)

    # DEBUG: which countries have no apple data
    no_data = []
    starttime_total = datetime.datetime.now() # timestamp

    rss_params = {
        'region': '',
        'media': 'apple-music',
        'chart': 'top-songs',
        'genre': 'all',
        'limit': 200,
    }

    service_name = 'Apple'
    for region in REGIONS:
        # debug:
        starttime = datetime.datetime.now() # timestamp
        print('Starting processing at', starttime.strftime('%H:%M:%S %m-%d-%y')) # timestamp

        try:
            rss_params['region'] = region
            url = RSS_url.format(**rss_params)

            print('Loading charts for region "%s" "...' % (region))
            req = Request(url)
            r = urlopen(req).read().decode('UTF-8')
            raw_data = json.loads(r)

            results = raw_data['feed']['results']
            print(results[0])

            if not raw_data:
                no_data.append(region)
                print('No download available, skipping...')
                print('There are now {} regions without data.'.format(len(no_data)))
                print('-' * 40)
                continue
            print('Found {} tracks.'.format(len(results)))

        except HTTPError as err:
            if err.code == 400:
                print('HTTP 400' )
            if err.code == 404:
                no_data.append(region) # country data for the chart is not available
                print('No RSS feed data found for {}'.format(region))


        # append data to Spotify API response
        # tracks = append_track_hash_from_db(region_data)
        # print('Getting track data from Spotify "Tracks" API...')
        # tracks = append_track_data(region_data)
        # print('Getting label and release date from Spotify "Albums" API...')
        # tracks = append_track_album_data(tracks)
        # print('Getting genre tags from Spotify "Artists" API...')
        # tracks = append_artist_data(tracks)
        # print('Processed %i tracks, adding to database' % len(tracks))
        # added = db.add_tracks(tracks, date_str, service_name)

        # write data to DB
        # db.set_processed(url)

        # timestamp
        endtime = datetime.datetime.now()
        processtime = endtime - starttime
        processtime_running_total = endtime - starttime_total
        print('Finished processing at', endtime.strftime('%H:%M:%S %m-%d-%y'))
        print('Processing time: %i minutes, %i seconds' % divmod(processtime.days *86400 + processtime.seconds, 60))
        print('Running processing time: %i minutes, %i seconds' % divmod(processtime_running_total.days *86400 + processtime_running_total.seconds, 60))
        print('-' * 40)

    # timestamp
    endtime_total = datetime.datetime.now()
    processtime_total = endtime_total - starttime_total
    print('Finished processing all regions at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
    print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days *86400 + processtime_total.seconds, 60))
    print('-' * 40)

    # no data
    print('no data for {} countries: {}'.format(len(no_data), no_data))

if __name__ == '__main__':
    # setup Apple api
    apple = Apple()

    # are http requests being cached?
    #CACHE_ENABLED = True
    cache_msg = '\033[92m enabled' if CACHE_ENABLED else '\033[91m disabled'
    print('HTTP cache is%s\033[0m' % cache_msg)

    # setup db
    # db = TrackDatabase(DATABASE_FILE)

    # prompt for date/mode
    # while True:
    #     mode = input('\nEnter a date (YYYY-MM-DD) or use "all|watch|latest": ')
    #     mode = mode.lower()
    #     if re.match(r'\d{4}-\d{2}-\d{2}|all|watch|latest', mode):
    #         break
    #     else:
    #         print('Invalid date, try again.')
    #
    # print('-' * 40)
    # print()

    mode = 'test'
    # while True:
    process(mode)

    #
    #     if mode == 'watch':
    #         print()
    #         print('=' * 40)
    #         print('\033[95mWatch cycle complete for all regions, starting over...\033[0m')
    #         print('=' * 40)
    #         print()
    #     else:
            # break
    print('Finished')