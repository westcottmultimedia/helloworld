import sqlite3, csv, codecs, re, json, os, base64, time, hashlib, ssl, jwt, configparser, logging
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError
from lxml import html
from pprint import pprint
from datetime import datetime, date
from dateutil import parser

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
DATABASE_FILE = '../test-apple.db'

# the Apple API url
# ie. https://api.music.apple.com/v1/catalog/{storefront}/genres/{id}
API_url = 'https://api.music.apple.com/v1/catalog/{region}/{media}'
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
            time_now = datetime.now()
            time_expired = datetime.now() + datetime.timedelta(hours=12)
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
            pprint(response, depth=3)

            # write data to file
            # file = open(cache_file, 'w+')
            # file.write(data)
            # print('written!')

            # debug:
            # pprint(response['results']['songs'][0]['data'][0]['attributes'], depth=2)
            # test
            # songs = response['results']['songs'][0]['data']
            # for song in songs:
            #     print(song['attributes']['name'])

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

def append_track_id_from_db(items):
    """
    Input:
        tracks: dict
    Output:
        tracks: dict
    Appends key 'track_id_db' key with db lookup value.
    This removes redundancy from apple API calls to retrieve info already in the DB.
    """
    # NOTE: hardcoded in service_id = 2 for Apple, may want to change to lookup from service table on join
    # service_id = self.get_service_id(service_name)
    for apple_track_id in items:
        query = """
            SELECT id
            FROM track
            WHERE service_id = 2
            AND service_track_id = (?)
        """
        # NOTE: there should be a one-to-one relationship between apple track_id and db id
        row = db.c.execute(query, [items[apple_track_id]['track_id']]).fetchone()
        if row:
            items[apple_track_id]['track_id_db'] = row[0]

    # for key in tracks:
    #     logging.debug("TRACKS--------", tracks[key])

    return items

def append_track_data(items, region):
    """
    Input:
        tracks: dict
    Output:
        tracks: dict +
            { 'isrc': xx, 'album_id': xx, 'artist_id': xx} for any track without a 'track_id_db'
    """
    apple = Apple()
    endpoint = API_url.format(region=region, media ='songs') + '?ids={}'

    tracks_to_lookup = [apple_id for apple_id, item in items.items() if 'track_id_db' not in item]

    id_str = ','.join(map(str, tracks_to_lookup))

    # retrieve API data and convert to easy lookup format
    r = apple.request(endpoint.format(id_str))
    data = r['data']
    r_dict = {item['id']: item for item in data} # construct dictionary with id as key

    for apple_id in r_dict:
        items[apple_id]['isrc'] = r_dict[apple_id]['attributes']['isrc']
        items[apple_id]['album_id'] = r_dict[apple_id]['relationships']['albums']['data'][0]['id']
        tracks[apple_id]['track_genres'] = r_dict[apple_id]['attributes']['genreNames']
    count = 0

    # diagnostics and statistics for printing
    for apple_id in items:
        if all (key in items[apple_id] for key in ('isrc', 'album_id', 'artist_id')):
            count += 1
        print('{} new items with isrc, album_id'.format(count))

    return items

def append_track_album_data(tracks):
    """
    Input:
        tracks: dict (with 'album_id' key, which refers to apple albumId)
    Output:
        tracks: dict +
            {'label': xx} for any track with 'albumId' key
    Append the label to tracks using the Apple albums API
    """
    apple = Apple()
    endpoint = API_url.format(region=region, media ='albums') + '?ids={}'

    albums_to_lookup = [track['album_id'] for k,track in tracks.items() if 'album_id' in track]

    id_str = ','.join(map(str, albums_to_lookup))

    # retrieve API data and convert to easy lookup format
    r = apple.request(endpoint.format(id_str))
    data = r['data']
    r_dict = {track['id']: track for track in data} # construct dictionary with id as key

    for apple_id in r_dict:
        tracks[apple_id]['album_name'] = r_dict[apple_id]['attributes']['name']
        tracks[apple_id]['label'] = r_dict[apple_id]['attributes']['recordLabel']
        tracks[apple_id]['album_release_date'] = r_dict[apple_id]['attributes']['releaseDate']
        tracks[apple_id]['album_genres'] = r_dict[apple_id]['attributes']['genreNames']
    return tracks

def append_artist_data(tracks):
    """
    Append the genre tags to tracks
    """
    apple = Apple()
    endpoint = API_url.format(region=region, media ='albums') + '?ids={}'

    artists_to_lookup = [track['artist_id'] for k,track in tracks.items() if 'album_id' in track]

    id_str = ','.join(map(str, albums_to_lookup))

    # retrieve API data and convert to easy lookup format
    r = apple.request(endpoint.format(id_str))
    data = r['data']
    r_dict = {track['id']: track for track in data} # construct dictionary with id as key

    for apple_id in r_dict:
        tracks[apple_id]['genres'] = r_dict[apple_id]['attributes']['genreNames']
    return tracks

# TrackDatabase class start
#
#
class TrackDatabase(object):
    """ SQLite Database Manager """
    def __init__(self, db_file='test.db'):
        super(TrackDatabase, self).__init__()
        self.db_file = db_file

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

        self.c.execute(
            stats_query,
            [track_hash, territoryId, serviceId, added, last_seen, peak_rank, peak_date]
        )
    def add_items(self, track_list, date_str, region, service_name):
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

            # track doesn't have track.id and the data for the track and album were retrieved from Apple API
            else:

                try:
                    # check if artist or album are in the db
                    # mapping from API response keys to values to input to db
                    artist_name = str(track['artistName'])
                    service_artist_id = str(track['artistId'])
                    track_release_date = str(track['releaseDate'])
                    collectionName = str(track['collectionName']) # Apple's proxy for album name? use this variable to answer that.

                    # other mapping to variables
                    service_album_id = str(track['album_id'])
                    isrc = str(track['isrc'])
                    artist_id = self.get_artist_id(service_id, service_artist_id)
                    album_id = self.get_album_id(service_id, service_album_id)
                    album_name = str(track['album_name'])

                    if (collectionName != album_name):
                        logging.debug('{}, {}', collectionName, album_name)

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

                    # add album if not in the db
                    if not album_id:
                        self.c.execute('''
                            INSERT OR IGNORE INTO album
                            (service_id, artist_id, service_album_id, album, release_date, label)
                            VALUES
                            (?, ?, ?, ?, ?, ?)
                        ''', (service_id, artist_id, service_album_id, album_name, str(track['release_date']), str(track['label']) )
                        )
                        album_id = self.c.lastrowid
                        print('Album added: {} for {}'.format(album_name, artist_name))

                    # add genres for artist
                    for genre in track['genres']:
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
                        (service_id, track_id, artist_id, album_id, str(track['Track Name']), isrc)
                    )
                    print('Track added: {} by {}'.format(str(track['Track Name']), artist_name))

                    track_id_db = self.c.lastrowid

                except Exception as e:
                    print(e)
                    raise

            # update peak_track_position table
            self.update_track_stats(service_id, territory_id, track_id_db, position, date_str)

            # update track_position table
            self.c.execute('''
                INSERT OR IGNORE INTO track_position
                (service_id, territory_id, track_id, isrc, position, stream_count, date_str)
                VALUES
                (?, ?, ?, ?, ?, ?, ?)
            ''', (service_id, territory_id, track_id_db, isrc, position, track['Streams'], date_str)
            )

            self.db.commit()


        return True

    def track_to_tuple(self, track):
        """
        Convert a track dict into a tuple
        """
        # hashed = self.get_row_hash(track)
        return (
            # hashed,
            str(track['Track Name']),
            str(track['Artist']),
            str(track['label']),
            str(track['isrc']),
            str(track['released']),
            str(track['genres'])
        )

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

def process(mode):
    """
    Process each region
    """
    service_name = 'Apple'
    number_results = 50
    limit = '&limit={}'.format(number_results)

    # DEBUG: which countries have no apple data
    no_data = []
    starttime_total = datetime.now() # timestamp

    rss_params = {
        'region': '',
        'media': 'apple-music',
        'chart': 'top-songs',
        'genre': 'all',
        'limit': 200,
    }

    for region in REGIONS:
        starttime = datetime.now() # timestamp
        print('Starting processing at', starttime.strftime('%H:%M:%S %m-%d-%y')) # timestamp

        rss_params['region'] = region
        url = RSS_url.format(**rss_params)

        try:
            req = Request(url)
            r = urlopen(req).read().decode('UTF-8')


            if not raw_data:
                no_data.append(region)
                print('No download available, skipping...')
                print('There are now {} regions without data.'.format(len(no_data)))
                print('-' * 40)
                continue
            print('Found {} tracks.'.format(len(results)))

        except HTTPError as err:
            if err.code == 400:
                print('HTTP 400')
            if err.code == 404:
                no_data.append(region) # country data for the chart is not available
                print('No RSS feed data found for {}'.format(region))

        # Process the data
        print('Loading charts for region "%s" "...' % (region))
        raw_data = json.loads(r)
        # parse date from RSS feed's last updated timestamp, make that the date for chart
        date_str = parser.parse(raw_data.updated).strftime('%Y-%m-%d')

        results = raw_data['feed']['results']
        items = {}
        # append position based on list index
        # convert list to dictionary for easier lookup, key is apple id
        for i, result in enumerate(results):
            result['position'] = i + 1
            items[result['id']] = result

        print(items)

        # append data to Apple data
        print('Looking up existing id in db')
        items = append_track_id_from_db(items)
        print('Getting track data from Apple "Tracks" API...')
        items = append_track_data(items, region)
        print('Getting label and release date from Apple "Albums" API...')
        items = append_track_album_data(items)
        # NOTE: no need to append genres here. They exist in RSS feed response
        # print('Getting genre tags from Apple "Artists" API...')
        # items = append_artist_data(items)
        print('Processed %i items, adding to database' % len(items))
        added = db.add_items(items, date_str, region, service_name)

        # write data to DB
        # db.set_processed(url)

        # timestamp
        endtime = datetime.now()
        processtime = endtime - starttime
        processtime_running_total = endtime - starttime_total
        print('Finished processing at', endtime.strftime('%H:%M:%S %m-%d-%y'))
        print('Processing time: %i minutes, %i seconds' % divmod(processtime.days *86400 + processtime.seconds, 60))
        print('Running processing time: %i minutes, %i seconds' % divmod(processtime_running_total.days *86400 + processtime_running_total.seconds, 60))
        print('-' * 40)

    # timestamp
    endtime_total = datetime.now()
    processtime_total = endtime_total - starttime_total
    print('Finished processing all regions at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
    print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days *86400 + processtime_total.seconds, 60))
    print('-' * 40)

    # no data
    print('no data for {} countries: {}'.format(len(no_data), no_data))

if __name__ == '__main__':
    # setup Apple api
    apple = Apple()
    endpoint = API_url.format(region='us', media ='albums') + '?ids={}'.format('1285853925,1294530286')
    r = apple.request(endpoint)
    print(r)
    # 1285853925 60291774
    # are http requests being cached?
    #CACHE_ENABLED = True
    cache_msg = '\033[92m enabled' if CACHE_ENABLED else '\033[91m disabled'
    print('HTTP cache is%s\033[0m' % cache_msg)

    # setup db
    db = TrackDatabase(DATABASE_FILE)

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
    # process(mode)

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
