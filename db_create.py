import sqlite3
DATABASE_NAME = 'v6.db'

# sqlite database filename/path
DATABASE_FILE = '../{}'.format(DATABASE_NAME)

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
                isrc text NOT NULL,
                UNIQUE(service_id, service_track_id) ON CONFLICT IGNORE,
                FOREIGN KEY (service_id) REFERENCES service(id),
                FOREIGN KEY (artist_id) REFERENCES artist(id),
                FOREIGN KEY (album_id) REFERENCES album(id)
            )
        ''')

        # artist table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS artist (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                service_artist_id text NOT NULL,
                artist text NOT NULL,
                UNIQUE(service_id, service_artist_id) ON CONFLICT IGNORE
            )
        ''')

        # artist_genre mapping table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS artist_genre (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                artist_id integer NOT NULL,
                genre text NOT NULL,
                UNIQUE(service_id, artist_id, genre) ON CONFLICT IGNORE,
                FOREIGN KEY (service_id) REFERENCES service(id),
                FOREIGN KEY (artist_id) REFERENCES artist(id)
            )
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
                release_date text NOT NULL,
                UNIQUE(service_id, service_album_id, artist_id) ON CONFLICT IGNORE,
                FOREIGN KEY (service_id) REFERENCES service(id),
                FOREIGN KEY (artist_id) REFERENCES artist(id)
            )
        ''')

        # music video table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS music_video (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                service_music_video_id integer NOT NULL,
                artist_id integer NOT NULL,
                music_video text NOT NULL,
                UNIQUE(service_id, service_music_video_id) ON CONFLICT IGNORE,
                FOREIGN KEY (artist_id) REFERENCES artist(id)
            )
        ''')

        # processed table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS processed (
                url text PRIMARY KEY,
                UNIQUE(url) ON CONFLICT IGNORE
            )
        ''')

        # service table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS service (
                id integer PRIMARY KEY,
                service_name text NOT NULL,
                UNIQUE(service_name) ON CONFLICT IGNORE
            )
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
                peak_date text NOT NULL,
                UNIQUE(service_id, territory_id, track_id) ON CONFLICT IGNORE,
                FOREIGN KEY (service_id) REFERENCES service(id),
                FOREIGN KEY (territory_id) REFERENCES territory(id),
                FOREIGN KEY (track_id) REFERENCES track(id)
            )
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
                date_str text NOT NULL,
                UNIQUE(service_id, territory_id, track_id, date_str) ON CONFLICT IGNORE,
                FOREIGN KEY (service_id) REFERENCES service(id),
                FOREIGN KEY (territory_id) REFERENCES territory(id),
                FOREIGN KEY (track_id) REFERENCES track(id)
            )
        ''')

        # territory table
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS territory (
                id integer PRIMARY KEY,
                code varchar(10) NOT NULL,
                name text NOT NULL,
                UNIQUE(code, name) ON CONFLICT IGNORE
            )
        ''')

        self.db.commit()

if __name__ == '__main__':

    db = TrackDatabase(DATABASE_FILE)
