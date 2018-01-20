import psycopg2, csv, datetime, sys
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

class TrackDatabase(object):

    def __init__(self):
        super(TrackDatabase, self).__init__()
        self.init_database()

    def init_database(self):
        # AWS
        # rds_host  = "beats.cekfuk4kqawy.us-west-2.rds.amazonaws.com"
        # name = "beatsdj"
        # password = "beatsdj123"
        # db_name = "beats"

        # LOCAL
        db_name ='beats_backup'
        name= 'ericchen0121'
        password = '123'

        try:
            # print('Connecting to the RDS PostgreSQL database {}...'.format(rds_host))
            # AWS
            # self.db = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name)

            # LOCAL BELOW Only
            self.db = psycopg2.connect(user=name, password=password, dbname=db_name)
            # print('Successfully connected to AWS RDS PostgreSQL instance.', rds_host)
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

    # TRACKS ADDED FROM YESTERDAY
    #
    def tp_add(self, date_to_process, refresh = False):

        if refresh:
            self.c.execute("""
                REFRESH materialized VIEW tp_add
            """
            )
        else:
            # takes 6.5s on localhost
            self.c.execute("""
                CREATE materialized VIEW IF NOT EXISTS tp_add as (
                    WITH add_view AS (
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

                    SELECT * FROM add_view
                    WHERE add_drop = 'add'
                )
            """.format(date_to_process))

    def tp_drop(self, date_to_process, refresh = False):
        if refresh:
            self.c.execute("""
                REFRESH materialized VIEW tp_drop
            """
            )
        else:
            # takes 46s on localhost
            self.c.execute("""
                CREATE materialized VIEW IF NOT EXISTS tp_drop as (
                    WITH drop_view AS (
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

                    SELECT * FROM drop_view
                    WHERE add_drop = 'drop'
                )
            """.format(date_to_process))


    def tp_movement(self, date_to_process):
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
                    END add_drop
                FROM track_position T1
                INNER JOIN track_position T2
                    ON T1.isrc = T2.isrc
                    AND T1.territory_id = T2.territory_id
                    AND T1.service_id = T2.service_id
                    AND DATE(T1.date_str) - DATE(T2.date_str) = 1
                WHERE T1.date_str IN (SELECT '{}' from track_position)

                UNION

                -- Select and add the same columns from the add table
                SELECT
                    *,
                    (SELECT to_char(DATE('{}') - 1, 'yyyy-mm-dd')) as previous_date,
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
                    (SELECT '{}') as date_str,
                    (SELECT to_char(DATE('{}') - 1, 'yyyy-mm-dd')) as previous_date,
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


if __name__ == '__main__':
    db = TrackDatabase()

    # previous
    # conn = connect()
    # NOW: db.db

    # c = conn.cursor()
    # NOW: db.c
    #

    # # spotify streaming
    print('Generating Spotify stream reports')
    service_id = 1
    # print(latest_streamingDates)
    # generateStreamingReporting(service_id, latest_streamingDates[service_id])
    date_to_process = '2017-11-04'

    # refresh_all = True

    db.tp_add(date_to_process, refresh = False)
    db.tp_drop(date_to_process, refresh = False)
    db.tp_movement(date_to_process)

    # service_id, service_chart_id, table, query, columns = getStreamingTrackSpotifyQuery()
    # exportReport(service_id, service_chart_id, table, query, columns)
    # printDivider(40)
