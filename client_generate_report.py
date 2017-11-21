import sqlite3, csv, datetime
DATABASE_NAME = 'v8.db'

# sqlite database filename/path
DATABASE_FILE = '../{}'.format(DATABASE_NAME)
OUTPUT_FILE_TEMPLATE = './deliverables/client/{}_{}_{}_{}.csv'
CLIENT_NAME = 'border_city'

#
SERVICE_MAP = {1: 'spotify', 2: 'apple_music'}
service_chart_id = 1 # 1 is a placeholder for Songs/Tracks Top 200 chart

def getReportDate(service_id):
    query = '''
        SELECT max(date_str)
        FROM client_border_city_latest_table
        WHERE service_id = ?
        LIMIT 1
    '''
    row = c.execute(query, [service_id]).fetchone()
    return row[0] if row else datetime.date.today().isoformat()


def writeToFile(service_id):
    report_date = getReportDate(service_id)
    output_file = OUTPUT_FILE_TEMPLATE.format(CLIENT_NAME, report_date, "{0:0=3d}".format(int(service_chart_id)), "{0:0=3d}".format(int(service_id)) )

    # set stream_count to NULL if Apple charts for client (in DB, these values are INTs)
    #
    query_stream_count = ''
    if int(service_id) == 2:
        query_stream_count = 'NULL as '

    query = '''
        SELECT
            date_str,
            CASE
                WHEN territory_id = 'global' THEN 'zz'
                ELSE territory_id
            END territory_id,
            add_drop,
            previous_track_position,
            chart_position,
            track_isrc,
            track_name,
            artist_name,
            {}stream_count,
            peak_ranking,
            peak_ranking_date,
            url,
            label
        FROM client_border_city_latest_table
        WHERE service_id = ?
    '''.format(query_stream_count)

    print(query)

    c.execute(query, [service_id])
    rows = c.fetchall()

    with open(output_file, 'w') as f:
        writer = csv.writer(f)

        # write the header row
        writer.writerow([
            'date_str', 'territory_id', 'add_drop',
            'previous_track_position','chart_position', 'track_isrc', 'track_name',
            'artist_name', 'stream_count', 'peak_ranking',
            'peak_ranking_date', 'url', 'label'
        ])

        for row in rows:
            # write to file!
            writer.writerow(row)

    print('Finished writing file {}'.format(output_file))

def generateReporting(service_id, date_to_process):

    # NOTE: Don't need to drop and recreate all these views.
    #
    # Also, can generate it so that it is dependent on user input to define the date of reporting (thru terminal)
    #   For that functionality, will need to ensure peak date is not a more current date than the reporting date.
    #   This may require another view specifically to handle that case or some additional logic/processing
    #
    c.execute('''
        DROP VIEW IF EXISTS track_position_movement_today_all
    ''')
    c.execute('''
        DROP VIEW IF EXISTS tp_add_only
    ''')
    c.execute('''
        DROP VIEW IF EXISTS tp_drop_only
    ''')
    c.execute('''
        DROP VIEW IF EXISTS peak_track_date
    ''')
    c.execute('''
        DROP VIEW IF EXISTS track_position_movement_today_all
    ''')
    c.execute('''
        DROP VIEW IF EXISTS tracks_with_multiple_labels
    ''')
    c.execute('''
        DROP VIEW IF EXISTS tracks_with_multiple_labels_all_data
    ''')
    c.execute('''
        DROP VIEW IF EXISTS tracks_with_multiple_labels_merged
    ''')

    c.execute('''
        DROP VIEW IF EXISTS client_border_city_daily
    ''')

    #  Clear out tables
    c.execute('''
        DELETE FROM track_position_movement_today_all_table
    ''')

    c.execute('''
        DROP TABLE client_border_city_latest_table
    ''')



    #
    c.execute('''
        CREATE TABLE IF NOT EXISTS peak_track_position_date_table (
            id integer PRIMARY KEY,
            service_id integer NOT NULL,
            territory_id integer NOT NULL,
            track_id text NOT NULL,
            isrc text NOT NULL,
            peak_rank integer NOT NULL,
            peak_date text NOT NULL,
            UNIQUE(service_id, territory_id, track_id) ON CONFLICT IGNORE,
            FOREIGN KEY (service_id) REFERENCES service(id),
            FOREIGN KEY (territory_id) REFERENCES territory(id),
            FOREIGN KEY (track_id) REFERENCES track(id)
        )
    ''')

    # Track Position Only ones that have been ADDED
    c.execute('''
        CREATE TABLE IF NOT EXISTS tp_add_only_table (
            date_str text,
            track_position_id integer,
            service_id integer,
            territory_id integer,
            track_id integer,
            isrc text,
            today_position integer,
            add_drop text
        )
    ''')

    # Track Position Only ones that have been REMOVED or DROPPED from tables
    c.execute('''
        CREATE TABLE IF NOT EXISTS tp_drop_only_table (
            previous_date text,
            track_position_id integer,
            service_id integer,
            territory_id integer,
            track_id integer,
            isrc text,
            today_position integer,
            add_drop text
        )
    ''')

    c.execute('''
        DELETE FROM tp_add_only_table
    ''')

    c.execute('''
        DELETE FROM tp_drop_only_table
    ''')

    c.execute('''
        CREATE VIEW peak_track_date as
        select service_id, territory_id, track_id, isrc, min(position) as peak_rank, earliest_date as peak_date
        from
        (
            select service_id, territory_id, track_id, isrc, position, min(date_str) as earliest_date
            from
            track_position tp
            group by isrc, tp.position, tp.territory_id, tp.service_id
            ORDER BY position asc
        )
        group by isrc, territory_id, service_id
    ''')

    c.execute('''
        INSERT INTO peak_track_position_date_table (
            service_id, territory_id, track_id, isrc, peak_rank, peak_date
        )
        SELECT * FROM peak_track_date
    ''')

    #
    c.execute('''
        CREATE VIEW tp_add_only as
        select
            T1.date_str as date_str,
            T1.id as track_position_id,
            T1.service_id as service_id,
            T1.territory_id as territory_id,
            T1.track_id as track_id,
            T1.isrc,
            T1.position as today_position,
            CASE when T2.position is NULL then "add" else NULL END add_drop
        FROM track_position T1
        LEFT JOIN track_position T2
            ON T1.isrc = T2.isrc AND T1.territory_id = T2.territory_id
            AND T1.service_id = T2.service_id
            AND julianday(T1.date_str) - julianday(T2.date_str) = 1
        WHERE
            T1.date_str in (SELECT '{}' from track_position)
            AND add_drop = "add"
    '''.format(date_to_process))

    c.execute('''
        INSERT INTO tp_add_only_table SELECT * FROM tp_add_only
    ''')

    c.execute('''
        CREATE VIEW tp_drop_only as
        select T1.date_str as previous_date,
        T1.id as track_position_id,
        T1.service_id as service_id,
        T1.territory_id as territory_id,
        T1.track_id as track_id,
        T1.isrc,
        T1.position as today_position,
        CASE
            when T2.position is NULL then 'drop' else NULL
        END add_drop
        FROM track_position T1
        LEFT JOIN track_position T2
            ON T1.isrc = T2.isrc
            AND T1.territory_id = T2.territory_id
            AND T1.service_id = T2.service_id
            AND julianday(T1.date_str) - julianday(T2.date_str) = -1
        where T1.date_str in (SELECT date('{}', '-1 day') from track_position)
        AND add_drop = 'drop'
    '''.format(date_to_process))

    # Write to table to speed up
    c.execute('''
        INSERT INTO tp_drop_only_table SELECT * FROM tp_drop_only
    ''')

    c.execute('''
        CREATE VIEW track_position_movement_today_all as
            select T1.*,
            T2.date_str as previous_date,
            T2.position as previous_track_position,
            T2.position - T1.position as movement,
            CASE
                when T2.position is NULL
                then 'add' else NULL
            end add_drop
            from track_position T1
            INNER JOIN track_position T2
                ON T1.isrc = T2.isrc
                AND T1.territory_id = T2.territory_id
                AND T1.service_id = T2.service_id
                AND julianday(T1.date_str) - julianday(T2.date_str) = 1
            WHERE T1.date_str in (SELECT '{}' from track_position)

            UNION

            select *,
            (select date('{}', '-1 day') from track_position) as previous_date,
            -1 as previous_track_position,
            200 - T1.position as movement,
            'add' as add_drop
            FROM track_position T1
            WHERE id in (select track_position_id from tp_add_only_table)

            UNION

            select
            T1.id,
            T1.service_id,
            T1.territory_id,
            T1.track_id,
            T1.isrc,
            -1 as position,
            -1 as stream_count,
            (select '{}' from track_position) as date_str,
            date((select '{}' from track_position) , '-1 day') as previous_date,
            T1.position as previous_track_position, T1.position - 201 as movement,
            'drop' as add_drop
            from track_position T1

            where id in (select track_position_id from tp_drop_only_table)
            order by date_str desc, territory_id asc, position asc
    '''.format(date_to_process, date_to_process, date_to_process, date_to_process))

    c.execute('''
        CREATE TABLE IF NOT EXISTS track_position_movement_today_all_table (
            id integer PRIMARY KEY,
            service_id integer NOT NULL,
            territory_id integer NOT NULL,
            track_id text NOT NULL,
            isrc text NOT NULL,
            position integer NOT NULL,
            stream_count integer DEFAULT -1,
            date_str text NOT NULL,
            previous_date text NOT NULL,
            previous_track_position integer NOT NULL,
            movement integer NOT NULL,
            add_drop text,
            FOREIGN KEY (service_id) REFERENCES service(id),
            FOREIGN KEY (territory_id) REFERENCES territory(id),
            FOREIGN KEY (track_id) REFERENCES track(id)
        )
    ''')

    c.execute('''
        INSERT INTO track_position_movement_today_all_table (
            service_id, territory_id, track_id, isrc, position, stream_count, date_str, previous_date, previous_track_position, movement, add_drop
        )
        SELECT
            service_id, territory_id, track_id, isrc, position, stream_count, date_str, previous_date, previous_track_position, movement, add_drop
        FROM track_position_movement_today_all
    ''')

    c.execute('''
        CREATE VIEW tracks_with_multiple_labels as
        select
        album.service_id as service_id,
        track.isrc, track.track,
        count(*) from track
        inner join album ON album.id = track.album_id
        group by track.isrc
        having count(*) > 1
    ''')

    c.execute('''
        CREATE VIEW tracks_with_multiple_labels_all_data as
        select * from track_album
        where isrc in ( select isrc from tracks_with_multiple_labels )
    ''')

    c.execute('''
        CREATE VIEW tracks_with_multiple_labels_merged as
        select *, min(release_date) as earliest_release_date from tracks_with_multiple_labels_all_data
        where label not like '%digital%' group by isrc
    ''')


    c.execute('''
        CREATE VIEW client_border_city_daily as
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
        ptd.peak_rank as peak_ranking,
        ptd.peak_date as peak_ranking_date,
        ('https://open.spotify.com/track/' || track.service_track_id) as url,
        case
            when tp.isrc in (select isrc from tracks_with_multiple_labels_merged)
            then (select label from tracks_with_multiple_labels_merged where isrc = tp.isrc)
            else album.label
        end label
        FROM track_position_movement_today_all_table tp
        INNER JOIN peak_track_position_date_table ptd
            ON ptd.isrc = tp.isrc
            AND ptd.territory_id = tp.territory_id
            AND ptd.service_id = tp.service_id
        INNER JOIN service on service.id = tp.service_id
        INNER JOIN territory on territory.id = tp.territory_id
        INNER JOIN track on track.id = tp.track_id
        INNER JOIN artist on track.artist_id = artist.id
        INNER JOIN album on track.album_id = album.id
        ORDER BY service_id ASC, territory_id ASC, chart_position ASC
    ''')


    # NOTE: What are sensible defaults for NULL/NOT NULL, Default values?
    c.execute('''
        CREATE TABLE IF NOT EXISTS client_border_city_latest_table (
            date_str text,
            service_id integer NOT NULL,
            territory_id integer NOT NULL,
            add_drop text DEFAULT 'stay',
            previous_track_position integer DEFAULT -1,
            chart_position integer DEFAULT -1,
            track_isrc text DEFAULT '',
            track_name text DEFAULT '',
            artist_name text DEFAULT '',
            stream_count integer,
            peak_ranking integer DEFAULT -1,
            peak_ranking_date text DEFAULT '',
            url text DEFAULT '',
            label text DEFAULT '',
            UNIQUE(date_str, service_id, territory_id, track_isrc, track_name, url) ON CONFLICT IGNORE
        )
    ''')

    c.execute('''
        INSERT INTO client_border_city_latest_table select * from client_border_city_daily
    ''')

    db.commit()
    writeToFile(service_id)

# Returns a dict with key of service_id and the latest date for that service_id that's available
# ex. latest_dates = {1: '2017-11-14', 2: '2017-11-15'}
#
def getAndPrintStats():
    c.execute('''
        SELECT service_id, date_str, count(date_str)
        FROM track_position
        GROUP BY service_id, date_str
        ORDER BY date_str desc
		LIMIT 2
    ''')

    latest_dates = {}

    rows = c.fetchmany(2)
    for row in rows:
        print('Latest date for service_id {} is {} with {} records.'.format(row[0], row[1], row[2]))
        latest_dates[row[0]] = (row[1])

    return latest_dates

if __name__ == '__main__':
    db = sqlite3.connect(DATABASE_FILE)
    c = db.cursor()

    # stats
    latest_dates = getAndPrintStats()

    service_id = input('\nSelect latest report for service_id: 1 for Spotify, 2 for Apple: ')

    latest_date_for_service = latest_dates[int(service_id)] # based on index
    print('Generating report for {} on {}'.format(SERVICE_MAP[int(service_id)], latest_date_for_service))

    # timestamping
    starttime_total = datetime.datetime.now()
    print('Starting processing at', starttime_total.strftime('%H:%M:%S %m-%d-%y'))

    # ask user for spotify or apple? or date reporting
    # or music/music video/album
    # START PROCESSING
    generateReporting(service_id, latest_date_for_service)

    # timestamping
    endtime_total = datetime.datetime.now()
    processtime_total = endtime_total - starttime_total
    print('Ending processing at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
    print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days *86400 + processtime_total.seconds, 60))
