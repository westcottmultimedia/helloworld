import sys
sys.path.insert(0, './aws_packages') # local relative path of aws lambda packages for zipping

import psycopg2, csv, datetime
from datetime import datetime, date, timedelta
DATABASE_NAME = 'beats_backup'

# sqlite database filename/path

OUTPUT_FILE_TEMPLATE = './deliverables/client/psql_{}_{}_{}_{}.csv'
CLIENT_NAME = 'bam'

# NOTE: this should mirror the db table (ie. db table for bam client)
SERVICE_MAP = {
    1: 'spotify',
    2: 'apple music'
}

SERVICE_CHARTS = {
    1: 'track streaming',
    2: 'track sales',
    3: 'album sales',
    4: 'music video sales'
}

# TODAY is day in UTC - 8hrs, or PST
# https://julien.danjou.info/blog/2015/python-and-timezones
TODAY = (datetime.utcnow() - timedelta(hours=8)).strftime('%Y-%m-%d')
YESTERDAY = (datetime.utcnow() - timedelta(hours=32)).strftime('%Y-%m-%d')

class TrackDatabase(object):

    def __init__(self):
        super(TrackDatabase, self).__init__()
        self.init_database()

    def init_database(self):
        # AWS
        rds_host  = "beats.cekfuk4kqawy.us-west-2.rds.amazonaws.com"
        name = "beatsdj"
        password = "beatsdj123"
        db_name = "beats"

        # LOCAL
        # db_name ='beats_backup'
        # name= 'ericchen0121'
        # password = '123'

        try:

            # AWS BELOW
            print('Connecting to the RDS PostgreSQL database {}...'.format(rds_host))
            self.db = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name)
            print('Successfully connected to AWS RDS PostgreSQL instance.', rds_host)

            # LOCALHOST BELOW Only
            # self.db = psycopg2.connect(user=name, password=password, dbname=db_name)

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

    # MATERIALIZED VIEWS
    # http://www.postgresqltutorial.com/postgresql-materialized-views/

    def tp_drop_materialized_views(self):
        # dropping views in order matters, so you don't hit a "objects rely on" error
        self.c.execute("""
            DROP materialized VIEW IF EXISTS streaming CASCADE
        """)

        self.c.execute("""
            DROP materialized VIEW IF EXISTS tp_movement CASCADE
        """)

        self.c.execute("""
            DROP materialized VIEW IF EXISTS tp_add CASCADE
        """)

        self.c.execute("""
            DROP materialized VIEW IF EXISTS tp_drop CASCADE
        """)

    def sp_drop_materialized_views(self):
        # dropping views in order matters, so you don't hit a "objects rely on" error

        # SALES REPORTS FIRST
        self.c.execute("""
            DROP materialized VIEW IF EXISTS sales_songs CASCADE
        """)

        self.c.execute("""
            DROP materialized VIEW IF EXISTS sales_albums CASCADE
        """)

        self.c.execute("""
            DROP materialized VIEW IF EXISTS sales_music_videos CASCADE
        """)

        # SALES ADD/DROP/MOVEMENT TABLES NEXT
        self.c.execute("""
            DROP materialized VIEW IF EXISTS sp_movement
        """)

        self.c.execute("""
            DROP materialized VIEW IF EXISTS sp_add
        """)

        self.c.execute("""
            DROP materialized VIEW IF EXISTS sp_drop
        """)

    # ADDED, DROPPED and MOVED Position Functions
    #
    def tp_add(self, date_to_process):
        print('starting tp add view')
        # NOTE: opted to delete and recreate materialized views each time rather than a choice based on refresh or not
        # This decision comes down to a) cannot change the definition of a custom date to a max(date_str) dynamically,
        # you'd have to delete and create the materialized view anyways. b) you don't know whether the definition has max(date_str)
        # in it, so you have to complex queries to figure it out ("SELECT definition FROM pg_matviews"), which makes this function relatively unreadable
        self.c.execute("""
            CREATE materialized VIEW IF NOT EXISTS tp_add as (
                WITH tp_add_view AS (
                    SELECT
                        T1.date_str AS date_str,
                        T1.id AS track_position_id,
                        T1.service_id AS service_id,
                        T1.territory_id AS territory_id,
                        T1.track_id AS track_id,
                        T1.isrc,
                        T1.position AS today_position,
                        CASE
                            WHEN T2.position IS NULL THEN 'add'
                            ELSE NULL
                        END AS add_drop
                    FROM track_position T1
                    LEFT JOIN track_position T2
                        ON T1.isrc = T2.isrc
                        AND T1.territory_id = T2.territory_id
                        AND T1.service_id = T2.service_id
                        AND DATE(T1.date_str) - DATE(T2.date_str) = 1
                    WHERE
                        T1.date_str = '{}'
                )

                SELECT * FROM tp_add_view
                WHERE add_drop = 'add'
            )
        """.format(date_to_process))
        print('finished tp add view')

    def sp_add(self, date_to_process):
        self.c.execute("""
            CREATE materialized VIEW IF NOT EXISTS sp_add as (
                WITH sp_add_view AS (
                    select
                        T1.date_str as date_str,
                        T1.id as sales_position_id,
                        T1.service_id as service_id,
                        T1.territory_id as territory_id,
                        T1.media_id as media_id,
                        T1.media_type as media_type,
                        T1.position as today_position,
                        CASE
                            WHEN T2.position is NULL THEN 'add'
                            ELSE NULL
                        END AS add_drop
                    FROM sales_position T1
                    LEFT JOIN sales_position T2
                        ON T1.media_id = T2.media_id
                        AND T1.media_type = T2.media_type
                        AND T1.territory_id = T2.territory_id
                        AND T1.service_id = T2.service_id
                        AND DATE(T1.date_str) - DATE(T2.date_str) = 1
                    WHERE
                        T1.date_str = '{}'
                )

                SELECT * FROM sp_add_view
                WHERE add_drop = 'add'
            )
        """.format(date_to_process))
        print('finished SP add view')

    def latest_date(self):
        self.c.execute('SELECT max(date_str) FROM track_position')
        return self.c.fetchone()[0]

    def tp_drop(self, date_to_process):
        print('starting tp drop view')
        self.c.execute("""
            CREATE materialized VIEW IF NOT EXISTS tp_drop as (
                WITH tp_drop_view AS (
                    SELECT
                        T1.date_str as previous_date,
                        T1.id as track_position_id,
                        T1.service_id as service_id,
                        T1.territory_id as territory_id,
                        T1.track_id as track_id,
                        T1.isrc,
                        T1.position as previous_position,
                        CASE
                            WHEN T2.position is NULL then 'drop'
                            ELSE NULL
                        END AS add_drop
                        FROM track_position T1
                        LEFT JOIN track_position T2
                            ON T1.isrc = T2.isrc
                            AND T1.territory_id = T2.territory_id
                            AND T1.service_id = T2.service_id
                            AND DATE(T1.date_str) - DATE(T2.date_str) = -1
                        WHERE
                            DATE(T1.date_str) = date('{}') - 1
                )

                SELECT * FROM tp_drop_view
                WHERE add_drop = 'drop'
            )
        """.format(date_to_process))
        print('finished TP drop view')

    def sp_drop(self, date_to_process):
        print('starting SP drop view')
        self.c.execute("""
            CREATE materialized VIEW IF NOT EXISTS sp_drop as (
                WITH sp_drop_view AS (
                    SELECT T1.date_str AS previous_date,
                    T1.id AS sales_position_id,
                    T1.service_id AS service_id,
                    T1.territory_id AS territory_id,
                    T1.media_id AS media_id,
                    T1.media_type AS media_type,
                    T1.position AS previous_position,
                    CASE
                        WHEN T2.position IS NULL THEN 'drop'
                        ELSE NULL
                    END AS add_drop
                    FROM sales_position T1
                    LEFT JOIN sales_position T2
                        ON T1.media_id = T2.media_id
                        AND T1.media_type = T2.media_type
                        AND T1.territory_id = T2.territory_id
                        AND T1.service_id = T2.service_id
                        AND DATE(T1.date_str) - DATE(T2.date_str) = -1
                    WHERE
                        DATE(T1.date_str) = date('{}') - 1

                )
                SELECT * FROM sp_drop_view
                WHERE add_drop = 'drop'
            )
        """.format(date_to_process))
        print('finished SP drop view')

    def tp_movement(self, date_to_process):
        print('starting tp movement view')
        self.c.execute("""
            CREATE materialized VIEW IF NOT EXISTS tp_movement as (
                SELECT
                    T1.*,
                    T2.date_str as previous_date,
                    T2.position as previous_track_position,
                    T2.position - T1.position as movement,
                    CASE
                        WHEN T2.position is NULL THEN 'add'
                        ELSE NULL
                    END AS add_drop
                FROM track_position T1
                INNER JOIN track_position T2
                    ON T1.isrc = T2.isrc
                    AND T1.territory_id = T2.territory_id
                    AND T1.service_id = T2.service_id
                    AND DATE(T1.date_str) - DATE(T2.date_str) = 1
                WHERE T1.date_str IN (SELECT '{}'::text from track_position)

                UNION

                -- Select and add the same columns from the add table
                SELECT
                    *,
                    (SELECT to_char(DATE('{}'::text) - 1, 'yyyy-mm-dd')) as previous_date,
                    -1 as previous_track_position,
                    200 - T1.position as movement,
                    'add' as add_drop
                FROM track_position T1
                WHERE
                    id in (SELECT track_position_id FROM tp_add)

                UNION

                -- Select and add the same columns from the drop table
                SELECT
                    T1.id,
                    T1.service_id,
                    T1.territory_id,
                    T1.track_id,
                    T1.isrc,
                    -1 as position,
                    -1 as stream_count,
                    (SELECT '{}'::text) as date_str,
                    (SELECT to_char(DATE('{}'::text) - 1, 'yyyy-mm-dd')) as previous_date,
                    T1.position as previous_track_position, T1.position - 201 as movement,
                    'drop' as add_drop
                FROM track_position T1
                WHERE id in (SELECT track_position_id from tp_drop)

                -- Order them by territory
                ORDER BY
                    date_str DESC,
                    territory_id ASC,
                    position ASC
            )
        """.format(date_to_process, date_to_process, date_to_process, date_to_process))
        print('finished tp movement view')

    def sp_movement(self, date_to_process):
        print('starting sp movement view')
        self.c.execute("""
            CREATE materialized VIEW IF NOT EXISTS sp_movement as (
                SELECT
                    T1.*,
                    T2.date_str as previous_date,
                    T2.position as previous_sales_position,
                    T2.position - T1.position as movement,
                    CASE
                        WHEN T2.position is NULL THEN 'add'
                        ELSE NULL
                    END AS add_drop
                from sales_position T1
                INNER JOIN sales_position T2
                    ON T1.media_id = T2.media_id
                    AND T1.media_type = T2.media_type
                    AND T1.territory_id = T2.territory_id
                    AND T1.service_id = T2.service_id
                    AND DATE(T1.date_str) - DATE(T2.date_str) = 1
                WHERE T1.date_str IN (SELECT '{}'::text from sales_position)

                UNION

                -- Select and add the same columns from the add table
                SELECT
                    *,
                    (SELECT to_char(DATE('{}'::text) - 1, 'yyyy-mm-dd')) as previous_date,
                    -1 as previous_sales_position,
                    200 - T1.position as movement,
                    'add' as add_drop
                FROM sales_position T1
                WHERE
                    id in (SELECT sales_position_id FROM sp_add)

                UNION

                -- Select and add the same columns from the drop table
                SELECT
                    T1.id,
                    T1.service_id,
                    T1.territory_id,
                    T1.media_id,
                    T1.media_type,
                    -1 as position,
                    -1 as stream_count,
                    (SELECT '{}'::text) as date_str,
                    (SELECT to_char(DATE('{}'::text) - 1, 'yyyy-mm-dd')) as previous_date,
                    T1.position as previous_sales_position,
                    T1.position - 201 as movement,
                    'drop' as add_drop
                FROM sales_position T1
                WHERE id in (SELECT sales_position_id from sp_drop)

                -- Order them by territory
                ORDER BY
                    date_str DESC,
                    territory_id ASC,
                    position ASC
            )
        """.format(date_to_process, date_to_process, date_to_process, date_to_process))
        print('finished sp movement view')

    # NOTE: This is a long running query. Need not refresh this very often.
    #
    def tp_labels(self):
        self.c.execute('''
            CREATE materialized VIEW IF NOT EXISTS tracks_with_multiple_labels AS (
                SELECT
                    album.service_id AS service_id,
                    track.isrc,
                    track.track,
                    count(*) FROM track
                INNER JOIN album ON album.id = track.album_id
                GROUP BY track.isrc, album.service_id, track.track
                HAVING count(*) > 1
            )
        ''')

        self.c.execute('''
            CREATE materialized VIEW IF NOT EXISTS tracks_with_multiple_labels_all_data AS (
                SELECT
                    *
                FROM track_album
                WHERE isrc IN ( SELECT isrc FROM tracks_with_multiple_labels )
            )
        ''')

        self.c.execute('''
            CREATE materialized VIEW IF NOT EXISTS tracks_with_multiple_labels_merged AS (
                SELECT
                    isrc,
                    min(release_date) AS earliest_release_date
                FROM tracks_with_multiple_labels_all_data
                WHERE label NOT LIKE '%digital%'
                GROUP BY isrc
            )
        ''')

    def track_album(self):
        print('starting track_album view')
        self.c.execute('''
            CREATE materialized VIEW IF NOT EXISTS track_album AS (
                SELECT
                    track.track,
                    album.album,
                    album.label,
                    track.isrc,
                    album.release_date
                FROM track
                INNER JOIN album
                    ON album.id = track.album_id
            )
        ''')
        print('finished track_album view')

    def tp_report_streaming(self, refresh = True):
        print('starting Apple Music and Spotify STREAMING')
        # took 46s to execute
        output_table = 'streaming'
        create_query = """
            CREATE materialized VIEW IF NOT EXISTS {} as
            SELECT
            tp.date_str as date_str,
            service.id as service_id,
            territory.code as territory_id,
            tp.add_drop as add_drop,
            tp.previous_track_position as previous_track_position,
            tp.position as chart_position,
            tp.isrc as track_isrc,
            track.track as track_name,
            artist.artist as artist_name,
            tp.stream_count as stream_count,
            tpp.peak_rank as peak_ranking,
            tpp.peak_date as peak_ranking_date,
            CASE
                WHEN service.id = 1 THEN ('https://open.spotify.com/track/' || track.service_track_id)
                WHEN service.id = 2 THEN ('https://itunes.apple.com/album/' || album.service_album_id || '?=' || track.service_track_id)
                ELSE ''
            END AS url,
            CASE
                WHEN tp.isrc in (select isrc from tracks_with_multiple_labels_merged)
                THEN (select label from tracks_with_multiple_labels_merged where isrc = tp.isrc)
                ELSE album.label
            end AS label
            FROM tp_movement tp
            INNER JOIN track_position_peak tpp
                ON tpp.isrc = tp.isrc
                AND tpp.territory_id = tp.territory_id
                AND tpp.service_id = tp.service_id
            INNER JOIN service on service.id = tp.service_id
            INNER JOIN territory on territory.id = tp.territory_id
            INNER JOIN track on track.id = tp.track_id
            INNER JOIN artist on track.artist_id = artist.id
            INNER JOIN album on track.album_id = album.id
            ORDER BY service_id ASC, territory_id ASC, chart_position ASC
        """

        refresh_query = """
            REFRESH materialized VIEW {};
        """

        if refresh:
            self.c.execute(refresh_query.format(output_table))
        else:
            self.c.execute(create_query.format(output_table))

        print('finished streaming report view')

    def sp_report_songs(self, refresh = True):
        print('starting iTunes SALES for SONGS/TRACKS')
        # took 46s to execute
        output_table = 'sales_songs'
        create_query = """
            CREATE materialized VIEW IF NOT EXISTS {} as
                SELECT
                    sp.date_str as date_str,
                    'track' as type,
                    service.id as service_id,
                    territory.code as territory_id,
                    sp.add_drop as add_drop,
                    sp.previous_sales_position as previous_sales_position,
                    sp.position as position,
                    track.track as track_name,
                    album.album as album_name,
                    artist.artist as artist_name,
                    track.isrc as isrc,
                    sp.sales_count as sales_count,
                    spp.peak_rank as peak_ranking,
                    spp.peak_date as peak_ranking_date,
                    ('https://itunes.apple.com/' || territory.code || '/album/' || album.service_album_id || '?=' || track.service_track_id) as url,
                    album.label as label
                FROM sp_movement sp
                INNER JOIN sales_position_peak spp
                    ON spp.media_id = sp.media_id
                    AND spp.media_type = sp.media_type
                    AND spp.territory_id = sp.territory_id
                    AND spp.service_id = sp.service_id
                INNER JOIN service on service.id = sp.service_id
                INNER JOIN territory on territory.id = sp.territory_id
                INNER JOIN track on track.id = sp.media_id
                INNER JOIN artist on track.artist_id = artist.id
                INNER JOIN album on track.album_id = album.id
                WHERE sp.media_type = 'track'
                ORDER BY
                    service_id ASC,
                    territory_id ASC,
                    position ASC
        """

        refresh_query = """
            REFRESH materialized VIEW {};
        """

        if refresh:
            self.c.execute(refresh_query.format(output_table))
        else:
            self.c.execute(create_query.format(output_table))

        print('finished song/track sales report view')

    def sp_report_albums(self, refresh = True):
        print('starting iTunes SALES for ALBUMS')
        # took 46s to execute
        output_table = 'sales_albums'
        create_query = """
            CREATE materialized VIEW IF NOT EXISTS {} as
                SELECT
                    sp.date_str as date_str,
                    'album' as type,
                    service.id as service_id,
                    territory.code as territory_id,
                    sp.add_drop as add_drop,
                    sp.previous_sales_position as previous_sales_position,
                    sp.position as position,
                    artist.artist as artist_name,
                    album.album as album_name,
                    sp.sales_count as sales_count,
                    spp.peak_rank as peak_ranking,
                    spp.peak_date as peak_ranking_date,
                    ('https://itunes.apple.com/' || territory.code || '/album/' || album.service_album_id) as url,
                    album.label as label
                FROM sp_movement sp
                INNER JOIN sales_position_peak spp
                    ON spp.media_id = sp.media_id
                    AND spp.media_type = sp.media_type
                    AND spp.territory_id = sp.territory_id
                    AND spp.service_id = sp.service_id
                INNER JOIN service on service.id = sp.service_id
                INNER JOIN territory on territory.id = sp.territory_id
                INNER JOIN album on album.id = sp.media_id
                INNER JOIN artist on album.artist_id = artist.id
                WHERE sp.media_type = 'album'
                ORDER BY
                    service_id ASC,
                    territory_id ASC,
                    position ASC
        """

        refresh_query = """
            REFRESH materialized VIEW {};
        """

        if refresh:
            self.c.execute(refresh_query.format(output_table))
        else:
            self.c.execute(create_query.format(output_table))

        print('finished album sales report view')

    def sp_report_music_videos(self, refresh = True):
        print('starting iTunes SALES for MUSIC VIDEOS')
        # took 46s to execute
        output_table = 'sales_music_videos'
        create_query = """
            CREATE materialized VIEW IF NOT EXISTS {} as
                SELECT
                    sp.date_str as date_str,
                    'music_video' as type,
                    service.id as service_id,
                    territory.code as territory_id,
                    sp.add_drop as add_drop,
                    sp.previous_sales_position as previous_sales_position,
                    sp.position as position,
                    artist.artist as artist_name,
                    music_video.music_video as music_video_name,
                    music_video.isrc as isrc,
                    sp.sales_count as sales_count,
                    spp.peak_rank as peak_ranking,
                    spp.peak_date as peak_ranking_date,
                    ('https://itunes.apple.com/' || territory.code || '/music-video/' || music_video.service_music_video_id) as url
                FROM sp_movement sp
                INNER JOIN sales_position_peak spp
                    ON spp.media_id = sp.media_id
                    AND spp.media_type = sp.media_type
                    AND spp.territory_id = sp.territory_id
                    AND spp.service_id = sp.service_id
                INNER JOIN service on service.id = sp.service_id
                INNER JOIN territory on territory.id = sp.territory_id
                INNER JOIN music_video on music_video.id = sp.media_id
                INNER JOIN artist on music_video.artist_id = artist.id
                WHERE sp.media_type = 'music_video'
                ORDER BY
                    service_id ASC,
                    territory_id ASC,
                    position ASC,
                    previous_sales_position ASC
        """

        refresh_query = """
            REFRESH materialized VIEW {};
        """

        if refresh:
            self.c.execute(refresh_query.format(output_table))
        else:
            self.c.execute(create_query.format(output_table))

        print('finished music video sales report view')

    def report_spotify_streaming(self, refresh = True):
        input_table = 'streaming'
        output_table = 'report_spotify_streaming'
        service_id = 1
        create_query = """
            CREATE materialized VIEW IF NOT EXISTS {} AS
            SELECT
                date_str,
                CASE
                    WHEN territory_id = 'global' THEN 'zz'
                    ELSE territory_id
                END AS territory_id,
                add_drop,
                previous_track_position,
                chart_position,
                track_isrc,
                track_name,
                artist_name,
                stream_count,
                peak_ranking,
                peak_ranking_date,
                url,
                label
            FROM {}
            WHERE service_id = {}
        """

        refresh_query = """
            REFRESH materialized VIEW {};
        """

        if refresh:
            self.c.execute(refresh_query.format(output_table))
        else:
            self.c.execute(create_query.format(output_table, input_table, service_id))

    def report_apple_streaming(self, refresh = True):
        input_table = 'streaming'
        output_table = 'report_apple_streaming'
        service_id = 2
        create_query = """
            CREATE materialized VIEW IF NOT EXISTS {} AS
            SELECT
                date_str,
                CASE
                    WHEN territory_id = 'global' THEN 'zz'
                    ELSE territory_id
                END AS territory_id,
                add_drop,
                previous_track_position,
                chart_position,
                track_isrc,
                track_name,
                artist_name,
                NULL AS stream_count,
                peak_ranking,
                peak_ranking_date,
                url,
                label
            FROM {}
            WHERE service_id = {}
        """

        refresh_query = """
            REFRESH materialized VIEW {};
        """

        if refresh:
            self.c.execute(refresh_query.format(output_table))
        else:
            self.c.execute(create_query.format(output_table, input_table, service_id))

    def report_sales_songs(self, refresh = True):
        input_table = 'sales_songs'
        output_table = 'report_sales_songs'
        service_id = 2
        create_query = """
            CREATE materialized VIEW IF NOT EXISTS {} AS
            SELECT
                date_str,
                CASE
                    WHEN territory_id = 'global' THEN 'zz'
                    ELSE territory_id
                END territory_id,
                add_drop,
                previous_sales_position,
                position,
                track_name,
                album_name,
                artist_name,
                isrc,
                NULL as sales_count,
                peak_ranking,
                peak_ranking_date,
                url,
                label
            FROM {}
        """

        refresh_query = """
            REFRESH materialized VIEW {};
        """

        if refresh:
            self.c.execute(refresh_query.format(output_table))
        else:
            self.c.execute(create_query.format(output_table, input_table))

    def report_sales_albums(self, refresh = True):
        input_table = 'sales_albums'
        output_table = 'report_sales_albums'
        service_id = 2
        create_query = """
            CREATE materialized VIEW IF NOT EXISTS {} AS
            SELECT
                date_str,
                CASE
                    WHEN territory_id = 'global' THEN 'zz'
                    ELSE territory_id
                END territory_id,
                add_drop,
                previous_sales_position,
                position,
                artist_name,
                album_name,
                NULL as sales_count,
                peak_ranking,
                peak_ranking_date,
                url,
                label
            FROM {}
        """

        refresh_query = """
            REFRESH materialized VIEW {};
        """

        if refresh:
            self.c.execute(refresh_query.format(output_table))
        else:
            self.c.execute(create_query.format(output_table, input_table))

    def report_sales_music_videos(self, refresh = True):
        input_table = 'sales_music_videos'
        output_table = 'report_sales_music_videos'
        service_id = 2
        create_query = """
            CREATE materialized VIEW IF NOT EXISTS {} AS
            SELECT
                date_str,
                CASE
                    WHEN territory_id = 'global' THEN 'zz'
                    ELSE territory_id
                END territory_id,
                add_drop,
                previous_sales_position,
                position,
                artist_name,
                music_video_name,
                isrc,
                NULL as sales_count,
                peak_ranking,
                peak_ranking_date,
                url
            FROM {}
        """

        refresh_query = """
            REFRESH materialized VIEW {};
        """

        if refresh:
            self.c.execute(refresh_query.format(output_table))
        else:
            self.c.execute(create_query.format(output_table, input_table))

    # ----------------PLAYLIST POSITION REPORTING! ----------------
    # def report_playlist_position(self):
    #     query = """CREATE materialized VIEW IF NOT EXISTS position_view AS (
    #           SELECT
    #               playlist_track_position.date_str,
    #               p.name AS "playlist name",
    #               playlist_track_position.position,
    #               t.track,
    #               a.artist,
    #               po.service_owner_id AS "spotify owner",
    #               playlist_track_position.playlist_version,
    #               playlist_track_position.isrc,
    #               p.service_playlist_id AS "spotify playlist id",
    #               pf.followers
    #           FROM playlist_track_position
    #           INNER JOIN playlist p
    #               ON p.id = playlist_track_position.playlist_id
    #           INNER JOIN track t
    #               ON t.id = playlist_track_position.track_id
    #           INNER JOIN artist a
    #               ON a.id = t.artist_id
    #           INNER JOIN playlist_owner po
    #               ON po.id = p.owner_id
    #           INNER JOIN playlist_followers pf
    #               ON pf.playlist_id = p.id
    #           WHERE
    #               pf.followers IS NOT NULL
    #               AND
    #               pf.date_str = '2018-02-11'
    #           ORDER BY
    #               pf.followers desc, p.id, playlist_track_position.position ASC
    #         )
    #     """

# GENERATE REPORTING!!!! PROCESS IT ALL!
def generate_all_reports(date_str):
    generate_streaming_reports(date_str)
    generate_sales_reports(date_str)

# this is an all in one function... however for AWS 5 minute limit, we will break
# up the task into two separate fucntions
#
def generate_streaming_reports(date_str):

    # print(latest_streamingDates)
    # generateStreamingReporting(service_id, latest_streamingDates[service_id])
    # date_to_process = db.latest_date()

    generate_streaming_movement(date_str)
    generate_streaming_tables()
    generate_streaming_report_for_spotify()
    generate_streaming_report_for_apple_music()

    # TODO: setup Data Stream to export table to S3 bucket

    print(datetime.now().strftime('%-H:%M:%S'))

# On Lambda, this function takes about 2 minutes
#
def generate_streaming_movement(date_str):
    clean_streaming_tables()
    calculate_streaming_movement(date_str)

def clean_streaming_tables():
    db.tp_drop_materialized_views()

def calculate_streaming_movement(date_str):
    print(datetime.now().strftime('%-H:%M:%S'))

    db.tp_add(date_str)
    db.tp_drop(date_str)
    db.tp_movement(date_str)
    print(datetime.now().strftime('%-H:%M:%S'))

def generate_streaming_tables():
    # GENERATE TABLE FOR BOTH STREAMING REPORTS
    print(datetime.now().strftime('%-H:%M:%S'))
    print('Generating streaming table reporting...')
    db.tp_report_streaming(refresh = False)

def generate_streaming_report_for_spotify():
    print(datetime.now().strftime('%-H:%M:%S'))
    print('Writing Spotify stream report...')
    db.report_spotify_streaming(refresh = False)

def generate_streaming_report_for_apple_music():
    print(datetime.now().strftime('%-H:%M:%S'))
    print('Writing Apple Music stream report...')
    db.report_apple_streaming(refresh = False)

def generate_sales_reports(date_str):

    # GENERATE APPLE ITUNES SALES REPORTING
    # TODO: NOTE: 01-27-2018 Finished with reporting views.
    # Need to get queries, test them and see that they generate the correct reports

    clean_sales_tables()
    calculate_sales_movement(date_str)

    generate_sales_report_for_songs()
    generate_sales_report_for_albums()
    generate_sales_report_for_music_videos()

def clean_sales_tables():
    global db
    db.sp_drop_materialized_views()

def calculate_sales_movement(date_str):
    global db
    db.sp_add(date_str)
    db.sp_drop(date_str)
    db.sp_movement(date_str)

def generate_sales_report_for_songs():
    global db
    db.sp_report_songs(refresh = False)
    db.report_sales_songs(refresh = False)

def generate_sales_report_for_albums():
    global db
    db.sp_report_albums(refresh = False)
    db.report_sales_albums(refresh = False)

def generate_sales_report_for_music_videos():
    global db
    db.sp_report_music_videos(refresh = False)
    db.report_sales_music_videos(refresh = False)

if __name__ == '__main__':
    # #---------Setup
    global db
    db = TrackDatabase()

    # # --------Work Werk Werk Werk Work
    # generate_all_reports('2018-01-25')
    # generate_streaming_reports('2018-01-25')
    generate_sales_reports('2018-01-19')

    # test_one_sales_report('2018-01-19')
    # generate_sales_report_for_music_videos()

    # #----------Teardown
    db.close_database()


    # service_id, service_chart_id, table, query, columns = getStreamingTrackSpotifyQuery()
    # exportReport(service_id, service_chart_id, table, query, columns)
    # printDivider(40)

#-------  AWS STEP FUNCTION HANDLERS
#

def generate_reporting_for_sales_handler(event, context):
    # #---------Setup
    global db
    db = TrackDatabase()

    # WORK
    date_str = event.setdefault('date_str', YESTERDAY)
    generate_sales_reports(date_str)

    # #----------Teardown
    db.close_database()


def generate_reporting_for_streaming_handler(event, context):
    # #---------Setup
    global db
    db = TrackDatabase()

    # WORK
    # set default value, or grab date value if available on input
    date_str = event.setdefault('date_str', YESTERDAY)
    generate_streaming_reports(date_str)

    # #----------Teardown
    db.close_database()
