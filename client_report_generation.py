import sqlite3, csv, datetime
DATABASE_NAME = 'v5.1.backup6-output.db'

# sqlite database filename/path
DATABASE_FILE = '../{}'.format(DATABASE_NAME)

db = sqlite3.connect(DATABASE_FILE)
c = db.cursor()

def writeToFile():
    c.execute('SELECT * FROM client_border_city_latest_table')
    rows = c.fetchall()

    with open('output.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow([
            'date_str', 'service_id', 'territory_id', 'add_drop',
            'chart_position integer', 'track_isrc', 'track_name',
            'artist_name', 'stream_count', 'peak_ranking',
            'peak_ranking_date', 'url', 'label'
        ])

        for row in rows:
            writer.writerow(row)

def generateReporting():

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
        DROP VIEW IF EXISTS client_border_city_test
    ''')

    # Create views
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

    c.execute('''
        INSERT INTO peak_track_position_date_table (
            service_id, territory_id, track_id, isrc, peak_rank, peak_date
        )
        SELECT * FROM peak_track_date
    ''')

    #  VIEWS exist in the DB
    #
    c.execute('''
        CREATE VIEW tp_add_only as
        select T1.date_str as date_str, T1.id as track_position_id,
        T1.track_id as track_id, T1.territory_id as territory_id, T1.isrc,
        T1.position as today_position, CASE when T2.position is NULL then "add" else NULL END add_drop
        from track_position T1
        LEFT JOIN track_position T2
        ON T1.isrc = T2.isrc AND T1.territory_id = T2.territory_id
        AND T1.service_id = T2.service_id
        AND julianday(T1.date_str) - julianday(T2.date_str) = 1
        where T1.date_str in (SELECT max(date_str) from track_position)
        and add_drop = "add"
    ''')

    c.execute('''
        CREATE VIEW tp_drop_only as select T1.date_str as previous_date, T1.id as track_position_id,
        T1.track_id as track_id, T1.territory_id as territory_id, T1.isrc, T1.position as today_position,
        CASE when T2.position is NULL then 'drop' else NULL END add_drop
        from track_position T1
        LEFT JOIN track_position T2 ON T1.isrc = T2.isrc AND T1.territory_id = T2.territory_id
        AND T1.service_id = T2.service_id AND julianday(T1.date_str) - julianday(T2.date_str) = -1
        where T1.date_str in (SELECT date(max(date_str), '-1 day') from track_position) AND add_drop = 'drop'
    ''')


    c.execute('''
        CREATE VIEW track_position_movement_today_all as
            select T1.*,
            T2.date_str as previous_date,
            T2.position as previous_track_position,
            T2.position - T1.position as movement,
            CASE when T2.position is NULL then 'add' else NULL end add_drop
            from track_position T1
            INNER JOIN track_position T2
                ON T1.isrc = T2.isrc
                AND T1.territory_id = T2.territory_id
                AND T1.service_id = T2.service_id
                AND julianday(T1.date_str) - julianday(T2.date_str) = 1
            WHERE T1.date_str in (SELECT max(date_str) from track_position)

            UNION

            select *,
            (select date(max(date_str), '-1 day') from track_position) as previous_date,
            -1 as previous_track_position,
            200 - T1.position as movement,
            'add' as add_drop from
            track_position T1 where id in (select track_position_id from tp_add_only)

            UNION

            select
            T1.id,
            T1.service_id,
            T1.territory_id,
            T1.track_id,
            T1.isrc,
            -1 as position,
            -1 as stream_count,
            (select max(date_str) from track_position) as date_str,
            date((select max(date_str) from track_position) , '-1 day') as previous_date,
            T1.position as previous_track_position, T1.position - 201 as movement,
            'drop' as add_drop
            from track_position T1

            where id in (select track_position_id from tp_drop_only)
            order by date_str desc, territory_id asc, position asc
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS track_position_movement_today_all_table (
            id integer PRIMARY KEY,
            service_id integer NOT NULL,
            territory_id integer NOT NULL,
            track_id text NOT NULL,
            isrc text NOT NULL,
            position integer NOT NULL,
            stream_count integer NOT NULL DEFAULT -1,
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
        CREATE VIEW tracks_with_multiple_labels as select album.service_id as service_id, track.isrc, track.track, count(*) from track inner join album ON album.id = track.album_id group by track.isrc having count(*) > 1
    ''')

    c.execute('''
        CREATE VIEW tracks_with_multiple_labels_all_data as select * from track_album where isrc in ( select isrc from tracks_with_multiple_labels )
    ''')

    c.execute('''
        CREATE VIEW tracks_with_multiple_labels_merged as select *, min(release_date) as earliest_release_date from tracks_with_multiple_labels_all_data where label not like '%digital%' group by isrc
    ''')

    c.execute('''
        CREATE VIEW client_border_city_test as
        SELECT tp.date_str as date_str,
        service.id as service_id,
        territory.code as territory_id, tp.add_drop as add_drop,
        tp.previous_track_position as previous_track_position,
        tp.position as chart_position, tp.isrc as track_isrc,
        track.track as track_name,
        artist.artist as artist_name,
        tp.stream_count as stream_count,
        ptd.peak_rank as peak_ranking,
        ptd.peak_date as peak_ranking_date,
        ('https://open.spotify.com/track/' || track.service_track_id) as url,
        case when tp.isrc in (select isrc from tracks_with_multiple_labels_merged) then (select label from tracks_with_multiple_labels_merged where isrc = tp.isrc) else album.label end label
        FROM track_position_movement_today_all_table tp
        INNER JOIN peak_track_position_date_table ptd
        ON ptd.isrc = tp.isrc
        AND ptd.territory_id = tp.territory_id
        AND ptd.service_id = tp.service_id
        INNER JOIN service on service.id = tp.service_id
        INNER JOIN territory on territory.id = tp.territory_id
        INNER JOIN track on track.id = tp.track_id
        INNER JOIN artist on track.artist_id = artist.id
        INNER JOIN album on track.album_id = album.id GROUP by tp.isrc, tp.territory_id
        ORDER BY service_id ASC, territory_id ASC, chart_position ASC
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS client_border_city_latest_table (
            date_str text,
            service_id integer NOT NULL,
            territory_id integer NOT NULL,
            add_drop text DEFAULT 'stay',
            previous_track_position integer,
            chart_position integer DEFAULT -1,
            track_isrc text DEFAULT '',
            track_name text DEFAULT '',
            artist_name text DEFAULT '',
            stream_count integer NOT NULL DEFAULT -1,
            peak_ranking integer DEFAULT -1,
            peak_ranking_date text DEFAULT '',
            url text DEFAULT '',
            label text DEFAULT ''
        )
    ''')

    c.execute('''
        INSERT INTO client_border_city_latest_table select * from client_border_city_test
    ''')

    db.commit()
    writeToFile()

if __name__ == '__main__':
    starttime_total = datetime.datetime.now() # timestamping
    print('Starting processing at', starttime_total.strftime('%H:%M:%S %m-%d-%y'))
    # START PROCESSING
    generateReporting()

    endtime_total = datetime.datetime.now()
    processtime_total = endtime_total - starttime_total
    print('Ending processing at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
    print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days *86400 + processtime_total.seconds, 60))
