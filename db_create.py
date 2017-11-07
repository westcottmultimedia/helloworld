class TrackDatabase(object):
    """ SQLite Database Manager """
    def __init__(self, db_file='test-v5.db'):
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
                id integer PRIMARY KEY AUTOINCREMENT,
                service_id integer NOT NULL,
                service_track_id text NOT NULL,
                service_artist_id text NOT NULL,
                track text NOT NULL,
                isrc text NOT NULL
            )
        ''')

        # artist table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS artist (
                id integer PRIMARY KEY AUTOINCREMENT,
                service_id integer NOT NULL,
                service_artist_id text NOT NULL,
                artist text NOT NULL
            )
        ''')

        # artist_genre mapping table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS artist_genre (
                id integer PRIMARY KEY AUTOINCREMENT,
                service_id integer NOT NULL,
                service_artist_id text NOT NULL,
                genre text NOT NULL
            )
        ''')

        # album table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS album (
                id integer PRIMARY KEY AUTOINCREMENT,
                service_id integer NOT NULL,
                service_album_id text NOT NULL,
                album text NOT NULL,
                release_date text NOT NULL,
                label text NOT NULL
            )
        ''')

        # music video table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS music_video (
                id integer PRIMARY KEY AUTOINCREMENT,
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
                id integer PRIMARY KEY AUTOINCREMENT,
                service_name text NOT NULL
            )
        ''')

        # stats table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS peak_track_position (
                id integer PRIMARY KEY AUTOINCREMENT,
                track_id integer NOT NULL,
                territory_id integer NOT NULL,
                first_added text NOT NULL,
                last_seen text NOT NULL,
                peak_rank integer NOT NULL,
                peak_date text NOT NULL
            )
        ''')

        # track_position table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS track_position (
                id integer PRIMARY KEY AUTOINCREMENT,
                service_id integer NOT NULL,
                territory_id integer NOT NULL,
                track_id integer NOT NULL,
                position integer NOT NULL,
                stream_count integer NOT NULL DEFAULT 0,
                date_str text NOT NULL
            )
        ''')

        # territory table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS territory (
                id integer PRIMARY KEY AUTOINCREMENT,
                code varchar(10) NOT NULL,
                name text NOT NULL
            )
        ''')

        # TODO: PROBABLY DO NOT NEED THIS TABLE
        # track_service_info table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS track_service_info (
                track_id integer NOT NULL PRIMARY KEY,
                service_id integer NOT NULL,
                url text,
                service_track_id text,
                service_album_id text,
                service_music_video_id text
            )
        ''')

        # SEED DATABASE TABLES
        #
        # seed service table
        self.c.execute('''
            INSERT OR IGNORE INTO service
            (service_name)
            VALUES
            (?)
        ''', ('Spotify')
        )

        self.c.execute('''
            INSERT OR IGNORE INTO territory
            (code, name)
            VALUES
            (?, ?)
        ''', ('global', 'global')
        )
