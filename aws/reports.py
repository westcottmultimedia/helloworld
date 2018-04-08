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

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None

    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("dbname=beats_backup user=ericchen0121 password=123")
        conn.autocommit = True

     # close the communication with the PostgreSQL
        # cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            return conn
        return False

def getLatestDateFromTable(table, service_id):
    query = """
        SELECT max(date_str)
        FROM {}
        WHERE service_id = ?
        LIMIT 1
    """.format(table)
    c.execute(query, [service_id])
    row = c.fetchone()
    return row[0] if row else datetime.date.today().isoformat()

# Herding data
def generateStreamingReporting(service_id, date_to_process):

    # NOTE: Don't need to drop and recreate all these views.
    #
    # Also, can generate it so that it is dependent on user input to define the date of reporting (thru terminal)
    #   For that functionality, will need to ensure peak date is not a more current date than the reporting date.
    #   This may require another view specifically to handle that case or some additional logic/processing
    #
    c.execute("""
        DROP VIEW IF EXISTS track_position_movement_today_all
    """)
    c.execute("""
        DROP VIEW IF EXISTS tp_add_only
    """)
    c.execute("""
        DROP VIEW IF EXISTS tp_drop_only
    """)
    c.execute("""
        DROP VIEW IF EXISTS peak_track_date
    """)
    c.execute("""
        DROP VIEW IF EXISTS track_position_movement_today_all
    """)
    c.execute("""
        DROP VIEW IF EXISTS tracks_with_multiple_labels
    """)
    c.execute("""
        DROP VIEW IF EXISTS tracks_with_multiple_labels_all_data
    """)
    c.execute("""
        DROP VIEW IF EXISTS tracks_with_multiple_labels_merged
    """)

    c.execute("""
        DROP VIEW IF EXISTS client_border_city_daily
    """)

    #  Clear out tables
    c.execute("""
        DELETE FROM track_position_movement_today_all_table
    """)

    c.execute("""
        DROP TABLE IF EXISTS client_border_city_latest_table
    """)

    #
    c.execute("""
        CREATE TABLE IF NOT EXISTS peak_track_date_table (
            id integer PRIMARY KEY,
            service_id integer NOT NULL,
            territory_id integer NOT NULL,
            track_id text NOT NULL,
            isrc text NOT NULL,
            peak_rank integer NOT NULL,
            peak_date text NOT NULL,
            UNIQUE(service_id, territory_id, track_id),
            FOREIGN KEY (service_id) REFERENCES service(id),
            FOREIGN KEY (territory_id) REFERENCES territory(id),
            FOREIGN KEY (track_id) REFERENCES track(id)
        )
    """)

    # Track Position Only ones that have been ADDED
    c.execute("""
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
    """)

    # Track Position Only ones that have been REMOVED or DROPPED from tables
    c.execute("""
        CREATE TABLE IF NOT EXISTS tp_drop_only_table (
            previous_date text,
            track_position_id integer,
            service_id integer,
            territory_id integer,
            track_id integer,
            isrc text,
            previous_position integer,
            add_drop text
        )
    """)

    c.execute("""
        DELETE FROM tp_add_only_table
    """)

    c.execute("""
        DELETE FROM tp_drop_only_table
    """)

    # c.execute("""
    #     CREATE VIEW peak_track_date as
    #     select service_id, territory_id, track_id, isrc, min(position) as peak_rank, earliest_date as peak_date
    #     from
    #     (
    #         select service_id, territory_id, track_id, isrc, position, min(date_str) as earliest_date
    #         from
    #         track_position tp
    #         group by isrc, tp.position, tp.territory_id, tp.service_id
    #         ORDER BY position asc
    #     )
    #     group by isrc, territory_id, service_id
    # """)

    # Modified
    c.execute("""
        CREATE VIEW peak_track_date as
        select
            service_id,
            territory_id,
            track_id,
            isrc,
            min(position) as peak_rank,
            earliest_date as peak_date
        from
        (
            select
                service_id,
                territory_id,
                track_id,
                isrc,
                position,
                min(date_str) as earliest_date
            from (
                select * from track_position
                where (date_str::DATE) <= (%s::DATE)
            ) AS last_track_position
            GROUP BY
                isrc,
                position,
                territory_id,
                service_id
            ORDER BY position asc
        ) AS min_track_date
        group by
            isrc,
            territory_id,
            service_id
    """, [date_to_process])

    # WORKING ON IT
    select
        service_id,
        territory_id,
        track_id,
        isrc,
        min(position) OVER ( PARTITION by
            isrc,
            territory_id,
            service_id
        ) as peak_rank,
        earliest_date_by_posiiton as peak_date
    from
    (
        SELECT
            id,
        	service_id,
        	territory_id,
        	track_id,
        	isrc,
        	position,
        	min(date_str)

        OVER (
        	PARTITION by
        		isrc,
        		position,
        		territory_id,
        		service_id
        	) as earliest_date_by_posiiton

        FROM (
        	SELECT * FROM track_position
        	WHERE date_str::DATE <= '2017-03-01'::DATE
        ) track_position_report

        ORDER BY
        	territory_id asc,
        	track_id asc,
        	POSITION asc
    ) t

    # c.execute("""
    #     INSERT INTO peak_track_position_date_table (
    #         service_id, territory_id, track_id, isrc, peak_rank, peak_date
    #     )
    #     SELECT * FROM peak_track_date
    # """)
    #
    # #
    # c.execute("""
    #     CREATE VIEW tp_add_only as
    #     select
    #         T1.date_str as date_str,
    #         T1.id as track_position_id,
    #         T1.service_id as service_id,
    #         T1.territory_id as territory_id,
    #         T1.track_id as track_id,
    #         T1.isrc,
    #         T1.position as today_position,
    #         CASE when T2.position is NULL then "add" else NULL END add_drop
    #     FROM track_position T1
    #     LEFT JOIN track_position T2
    #         ON T1.isrc = T2.isrc AND T1.territory_id = T2.territory_id
    #         AND T1.service_id = T2.service_id
    #         AND julianday(T1.date_str) - julianday(T2.date_str) = 1
    #     WHERE
    #         T1.date_str = '{}'
    #         AND add_drop = "add"
    # """.format(date_to_process))
    #
    # c.execute("""
    #     INSERT INTO tp_add_only_table SELECT * FROM tp_add_only
    # """)
    #
    # c.execute("""
    #     CREATE VIEW tp_drop_only as
    #     select T1.date_str as previous_date,
    #     T1.id as track_position_id,
    #     T1.service_id as service_id,
    #     T1.territory_id as territory_id,
    #     T1.track_id as track_id,
    #     T1.isrc,
    #     T1.position as previous_position,
    #     CASE
    #         when T2.position is NULL then 'drop' else NULL
    #     END add_drop
    #     FROM track_position T1
    #     LEFT JOIN track_position T2
    #         ON T1.isrc = T2.isrc
    #         AND T1.territory_id = T2.territory_id
    #         AND T1.service_id = T2.service_id
    #         AND julianday(T1.date_str) - julianday(T2.date_str) = -1
    #     where T1.date_str = date('{}', '-1 day')
    #     AND add_drop = 'drop'
    # """.format(date_to_process))
    #
    # # Write to table to speed up
    # c.execute("""
    #     INSERT INTO tp_drop_only_table SELECT * FROM tp_drop_only
    # """)
    #
    # c.execute("""
    #     CREATE VIEW track_position_movement_today_all as
    #         select T1.*,
    #         T2.date_str as previous_date,
    #         T2.position as previous_track_position,
    #         T2.position - T1.position as movement,
    #         CASE
    #             when T2.position is NULL
    #             then 'add' else NULL
    #         end add_drop
    #         from track_position T1
    #         INNER JOIN track_position T2
    #             ON T1.isrc = T2.isrc
    #             AND T1.territory_id = T2.territory_id
    #             AND T1.service_id = T2.service_id
    #             AND julianday(T1.date_str) - julianday(T2.date_str) = 1
    #         WHERE T1.date_str in (SELECT '{}' from track_position)
    #
    #         UNION
    #
    #         select *,
    #         (select date('{}', '-1 day') from track_position) as previous_date,
    #         -1 as previous_track_position,
    #         200 - T1.position as movement,
    #         'add' as add_drop
    #         FROM track_position T1
    #         WHERE id in (select track_position_id from tp_add_only_table)
    #
    #         UNION
    #
    #         select
    #         T1.id,
    #         T1.service_id,
    #         T1.territory_id,
    #         T1.track_id,
    #         T1.isrc,
    #         -1 as position,
    #         -1 as stream_count,
    #         (select '{}' from track_position) as date_str,
    #         date((select '{}' from track_position) , '-1 day') as previous_date,
    #         T1.position as previous_track_position, T1.position - 201 as movement,
    #         'drop' as add_drop
    #         from track_position T1
    #
    #         where id in (select track_position_id from tp_drop_only_table)
    #         order by date_str desc, territory_id asc, position asc
    # """.format(date_to_process, date_to_process, date_to_process, date_to_process))
    #
    # c.execute("""
    #     CREATE TABLE IF NOT EXISTS track_position_movement_today_all_table (
    #         id integer PRIMARY KEY,
    #         service_id integer NOT NULL,
    #         territory_id integer NOT NULL,
    #         track_id text NOT NULL,
    #         isrc text NOT NULL,
    #         position integer NOT NULL,
    #         stream_count integer DEFAULT -1,
    #         date_str text NOT NULL,
    #         previous_date text NOT NULL,
    #         previous_track_position integer NOT NULL,
    #         movement integer NOT NULL,
    #         add_drop text,
    #         FOREIGN KEY (service_id) REFERENCES service(id),
    #         FOREIGN KEY (territory_id) REFERENCES territory(id),
    #         FOREIGN KEY (track_id) REFERENCES track(id)
    #     )
    # """)
    #
    # c.execute("""
    #     INSERT INTO track_position_movement_today_all_table (
    #         service_id, territory_id, track_id, isrc, position, stream_count, date_str, previous_date, previous_track_position, movement, add_drop
    #     )
    #     SELECT
    #         service_id, territory_id, track_id, isrc, position, stream_count, date_str, previous_date, previous_track_position, movement, add_drop
    #     FROM track_position_movement_today_all
    # """)
    #
    # c.execute("""
    #     CREATE VIEW tracks_with_multiple_labels as
    #     select
    #     album.service_id as service_id,
    #     track.isrc, track.track,
    #     count(*) from track
    #     inner join album ON album.id = track.album_id
    #     group by track.isrc
    #     having count(*) > 1
    # """)
    #
    # c.execute("""
    #     CREATE VIEW tracks_with_multiple_labels_all_data as
    #     select * from track_album
    #     where isrc in ( select isrc from tracks_with_multiple_labels )
    # """)
    #
    # c.execute("""
    #     CREATE VIEW tracks_with_multiple_labels_merged as
    #     select *, min(release_date) as earliest_release_date from tracks_with_multiple_labels_all_data
    #     where label not like '%digital%' group by isrc
    # """)
    #
    # c.execute("""
    #     CREATE VIEW client_border_city_daily as
    #     SELECT
    #     tp.date_str as date_str,
    #     service.id as service_id,
    #     territory.code as territory_id,
    #     tp.add_drop as add_drop,
    #     tp.previous_track_position as previous_track_position,
    #     tp.position as chart_position,
    #     tp.isrc as track_isrc,
    #     track.track as track_name,
    #     artist.artist as artist_name,
    #     tp.stream_count as stream_count,
    #     ptd.peak_rank as peak_ranking,
    #     ptd.peak_date as peak_ranking_date,
    #     CASE
    #         WHEN service.id = 1 THEN ('https://open.spotify.com/track/' || track.service_track_id)
    #         WHEN service.id = 2 THEN ('https://itunes.apple.com/album/' || album.service_album_id || '?=' || track.service_track_id)
    #         ELSE ''
    #     END url,
    #     CASE
    #         WHEN tp.isrc in (select isrc from tracks_with_multiple_labels_merged)
    #         THEN (select label from tracks_with_multiple_labels_merged where isrc = tp.isrc)
    #         ELSE album.label
    #     end label
    #     FROM track_position_movement_today_all_table tp
    #     INNER JOIN peak_track_position_date_table ptd
    #         ON ptd.isrc = tp.isrc
    #         AND ptd.territory_id = tp.territory_id
    #         AND ptd.service_id = tp.service_id
    #     INNER JOIN service on service.id = tp.service_id
    #     INNER JOIN territory on territory.id = tp.territory_id
    #     INNER JOIN track on track.id = tp.track_id
    #     INNER JOIN artist on track.artist_id = artist.id
    #     INNER JOIN album on track.album_id = album.id
    #     ORDER BY service_id ASC, territory_id ASC, chart_position ASC
    # """)
    #
    #
    # # NOTE: What are sensible defaults for NULL/NOT NULL, Default values?
    # c.execute("""
    #     CREATE TABLE IF NOT EXISTS client_border_city_latest_table (
    #         date_str text,
    #         service_id integer NOT NULL,
    #         territory_id integer NOT NULL,
    #         add_drop text DEFAULT 'stay',
    #         previous_track_position integer DEFAULT -1,
    #         chart_position integer DEFAULT -1,
    #         track_isrc text DEFAULT '',
    #         track_name text DEFAULT '',
    #         artist_name text DEFAULT '',
    #         stream_count integer,
    #         peak_ranking integer DEFAULT -1,
    #         peak_ranking_date text DEFAULT '',
    #         url text DEFAULT '',
    #         label text DEFAULT '',
    #         UNIQUE(date_str, service_id, territory_id, track_isrc, track_name, url)
    #     )
    # """)
    #
    # c.execute("""
    #     INSERT INTO client_border_city_latest_table select * from client_border_city_daily
    # """)

    db.commit()

def generateiTunesSalesReporting(service_id, date_to_process):

    # Sales Position Views
    #
    c.execute("""
        DROP VIEW IF EXISTS sp_add_only
    """)
    c.execute("""
        DROP VIEW IF EXISTS sp_drop_only
    """)

    c.execute("""
        DROP VIEW IF EXISTS sp_movement_today_all
    """)

    c.execute("""
        DROP VIEW IF EXISTS client_border_city_daily
    """)

    c.execute("""
        CREATE VIEW sp_add_only as
        select
            T1.date_str as date_str,
            T1.id as sales_position_id,
            T1.service_id as service_id,
            T1.territory_id as territory_id,
            T1.media_id as media_id,
            T1.media_type as media_type,
            T1.position as today_position,
            CASE
                when T2.position is NULL then "add" else NULL
            END add_drop
        FROM sales_position T1
        LEFT JOIN sales_position T2
            ON T1.media_id = T2.media_id
            AND T1.media_type = T2.media_type
            AND T1.territory_id = T2.territory_id
            AND T1.service_id = T2.service_id
            AND julianday(T1.date_str) - julianday(T2.date_str) = 1
        WHERE
            T1.date_str = '{}'
            AND add_drop = "add"
    """.format(date_to_process))

    c.execute("""
        CREATE VIEW sp_drop_only as
        select T1.date_str as previous_date,
        T1.id as sales_position_id,
        T1.service_id as service_id,
        T1.territory_id as territory_id,
        T1.media_id as media_id,
        T1.media_type as media_type,
        T1.position as previous_position,
        CASE
            when T2.position is NULL then 'drop' else NULL
        END add_drop
        FROM sales_position T1
        LEFT JOIN sales_position T2
            ON T1.media_id = T2.media_id
            AND T1.media_type = T2.media_type
            AND T1.territory_id = T2.territory_id
            AND T1.service_id = T2.service_id
            AND julianday(T1.date_str) - julianday(T2.date_str) = -1
        where T1.date_str in (SELECT date('{}', '-1 day'))
        AND add_drop = 'drop'
    """.format(date_to_process))

    # Sales position ADD and DROP only tables
    c.execute("""
        CREATE TABLE IF NOT EXISTS sp_add_only_table (
            date_str text,
            sales_position_id integer,
            service_id integer,
            territory_id integer,
            media_id integer,
            media_type text,
            today_position integer,
            add_drop text
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sp_drop_only_table (
            previous_date text,
            sales_position_id integer,
            service_id integer,
            territory_id integer,
            media_id integer,
            media_type text,
            today_position integer,
            add_drop text
        )
    """)

    # Clear data before inserting
    c.execute("""
        DELETE FROM sp_add_only_table
    """)
    #
    c.execute("""
        DELETE FROM sp_drop_only_table
    """)

    # Copy into Tables for faster lookups
    c.execute("""
        INSERT INTO sp_add_only_table SELECT * FROM sp_add_only
    """)

    c.execute("""
        INSERT INTO sp_drop_only_table SELECT * FROM sp_drop_only
    """)


    c.execute("""
        DROP VIEW IF EXISTS sp_movement_today_all
    """)

    c.execute("""
        CREATE VIEW sp_movement_today_all as
            select T1.*,
            T2.date_str as previous_date,
            T2.position as previous_sales_position,
            T2.position - T1.position as movement,
            CASE
                when T2.position is NULL
                then 'add' else NULL
            end add_drop
            from sales_position T1
            INNER JOIN sales_position T2
                ON T1.media_id = T2.media_id
                AND T1.media_type = T2.media_type
                AND T1.territory_id = T2.territory_id
                AND T1.service_id = T2.service_id
                AND julianday(T1.date_str) - julianday(T2.date_str) = 1
            WHERE T1.date_str in (SELECT '{}' from sales_position)

            UNION

            select *,
            (select date('{}', '-1 day') from sales_position) as previous_date,
            -1 as previous_sales_position,
            200 - T1.position as movement,
            'add' as add_drop
            FROM sales_position T1
            WHERE id in (select sales_position_id from sp_add_only)

            UNION

            select
            T1.id,
            T1.service_id,
            T1.territory_id,
            T1.media_id,
            T1.media_type,
            -1 as position,
            -1 as stream_count,
            (select '{}' from sales_position) as date_str,
            date((select '{}' from sales_position) , '-1 day') as previous_date,
            T1.position as previous_sales_position, T1.position - 201 as movement,
            'drop' as add_drop
            from sales_position T1

            where id in (select sales_position_id from sp_drop_only)
            order by date_str desc, territory_id asc, position asc
    """.format(date_to_process, date_to_process, date_to_process, date_to_process))

    c.execute("""
        CREATE TABLE IF NOT EXISTS sp_movement_today_all_table (
            id integer PRIMARY KEY,
            service_id integer NOT NULL,
            territory_id integer NOT NULL,
            media_id integer NOT NULL,
            media_type text NOT NULL,
            position integer NOT NULL,
            sales_count integer DEFAULT -1,
            date_str text NOT NULL,
            previous_date text NOT NULL,
            previous_sales_position integer NOT NULL,
            movement integer NOT NULL,
            add_drop text,
            FOREIGN KEY (service_id) REFERENCES service(id),
            FOREIGN KEY (territory_id) REFERENCES territory(id)
        )
    """)

    c.execute("""
        DELETE FROM sp_movement_today_all_table
    """)

    c.execute("""
        INSERT INTO sp_movement_today_all_table (
            service_id, territory_id, media_id, media_type, position, sales_count, date_str, previous_date, previous_sales_position, movement, add_drop
        )
        SELECT
            service_id, territory_id, media_id, media_type, position, sales_count, date_str, previous_date, previous_sales_position, movement, add_drop
        FROM sp_movement_today_all
    """)

    c.execute("""
        DROP VIEW IF EXISTS peak_sales_date
    """)

    # Original peak_sales_date view which calculated best position regardless of if the peak date was after the date to processed
    # The modified view filters out the sales position rows before or on the report date, to compute the peak date at that time
    # c.execute("""
    #     CREATE VIEW IF NOT EXISTS peak_sales_date as
    #     select
    #         service_id,
    #         territory_id,
    #         media_id,
    #         media_type,
    #         min(position) as peak_rank,
    #         earliest_date as peak_date
    #     from
    #     (
    #         select
    #             service_id,
    #             territory_id,
    #             media_id,
    #             media_type,
    #             position,
    #             min(date_str) as earliest_date
    #         from
    #         sales_position sp
    #         group by sp.media_id, sp.media_type, sp.position, sp.territory_id, sp.service_id
    #         ORDER BY position asc
    #     )
    #     group by media_id, media_type, territory_id, service_id
    # """)

    # # The modified view filters out the sales position rows before or on the report date, to compute the peak date at that time
    c.execute("""
        CREATE VIEW IF NOT EXISTS peak_sales_date as
        select
            service_id,
            territory_id,
            media_id,
            media_type,
            min(position) as peak_rank,
            earliest_date as peak_date
        from
        (
            select
                service_id,
                territory_id,
                media_id,
                media_type,
                position,
                min(date_str) as earliest_date
            from (
                select * from sales_position as sp
                where julianday(date_str) <= julianday('{}')
            )
            group by media_id, media_type, position, territory_id, service_id
            ORDER BY position asc
        )
        group by media_id, media_type, territory_id, service_id
    """.format(date_to_process))

    c.execute("""
        CREATE TABLE IF NOT EXISTS peak_sp_date_table (
            id integer PRIMARY KEY,
            service_id integer NOT NULL,
            territory_id integer NOT NULL,
            media_id text NOT NULL,
            media_type text NOT NULL,
            peak_rank integer NOT NULL,
            peak_date text NOT NULL,
            UNIQUE(service_id, territory_id, media_id, media_type),
            FOREIGN KEY (service_id) REFERENCES service(id),
            FOREIGN KEY (territory_id) REFERENCES territory(id)
        )
    """)

    c.execute("""
        DELETE FROM peak_sp_date_table
    """)

    c.execute("""
        INSERT INTO peak_sp_date_table (
            service_id,
            territory_id,
            media_id,
            media_type,
            peak_rank,
            peak_date
        )
        SELECT * FROM peak_sales_date
    """)

    c.execute("""
        DROP VIEW IF EXISTS bam_sales_track_daily
    """)

    c.execute("""
        CREATE VIEW IF NOT EXISTS bam_sales_track_daily as
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
            pspd.peak_rank as peak_ranking,
            pspd.peak_date as peak_ranking_date,
            ('https://itunes.apple.com/' || territory.code || '/album/' || album.service_album_id || '?=' || track.service_track_id) as url,
            album.label as label
        FROM sp_movement_today_all_table sp
        INNER JOIN peak_sp_date_table pspd
            ON pspd.media_id = sp.media_id
            AND pspd.media_type = sp.media_type
            AND pspd.territory_id = sp.territory_id
            AND pspd.service_id = sp.service_id
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
    """)

    c.execute("""
        DROP VIEW IF EXISTS bam_sales_album_daily
    """)

    c.execute("""
        CREATE VIEW IF NOT EXISTS bam_sales_album_daily as
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
            pspd.peak_rank as peak_ranking,
            pspd.peak_date as peak_ranking_date,
            ('https://itunes.apple.com/' || territory.code || '/album/' || album.service_album_id) as url,
            album.label as label
        FROM sp_movement_today_all_table sp
        INNER JOIN peak_sp_date_table pspd
            ON pspd.media_id = sp.media_id
            AND pspd.media_type = sp.media_type
            AND pspd.territory_id = sp.territory_id
            AND pspd.service_id = sp.service_id
        INNER JOIN service on service.id = sp.service_id
        INNER JOIN territory on territory.id = sp.territory_id
        INNER JOIN album on album.id = sp.media_id
        INNER JOIN artist on album.artist_id = artist.id
        WHERE sp.media_type = 'album'
        ORDER BY
            service_id ASC,
            territory_id ASC,
            position ASC
    """)

    c.execute("""
        DROP VIEW IF EXISTS bam_sales_music_video_daily
    """)

    c.execute("""
        CREATE VIEW IF NOT EXISTS bam_sales_music_video_daily as
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
            pspd.peak_rank as peak_ranking,
            pspd.peak_date as peak_ranking_date,
            ('https://itunes.apple.com/' || territory.code || '/music-video/' || music_video.service_music_video_id) as url
        FROM sp_movement_today_all_table sp
        INNER JOIN peak_sp_date_table pspd
            ON pspd.media_id = sp.media_id
            AND pspd.media_type = sp.media_type
            AND pspd.territory_id = sp.territory_id
            AND pspd.service_id = sp.service_id
        INNER JOIN service on service.id = sp.service_id
        INNER JOIN territory on territory.id = sp.territory_id
        INNER JOIN artist on music_video.artist_id = artist.id
        INNER JOIN music_video on music_video.id = sp.media_id
        WHERE sp.media_type = 'music_video'
        ORDER BY
            service_id ASC,
            territory_id ASC,
            position ASC
    """)

    db.commit()

# Report parameters
def getSalesTrackQuery():
    table = 'bam_sales_track_daily'
    query = """
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

    columns = [
        'date_str', 'territory_id', 'add_drop',
        'previous_sales_position','position', 'track_name', 'album_name',
        'artist_name', 'isrc', 'sales_count', 'peak_ranking',
        'peak_ranking_date', 'url', 'label'
    ]

    service_id = 2
    service_chart_id = 2

    return (service_id, service_chart_id, table, query, columns)

def getSalesAlbumQuery():
    table = 'bam_sales_album_daily'
    query = """
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

    columns = [
        'date_str', 'territory_id', 'add_drop',
        'previous_sales_position','position', 'artist_name', 'album_name',
        'sales_count', 'peak_ranking', 'peak_ranking_date', 'url', 'label'
    ]
    service_id = 2
    service_chart_id = 3
    return (service_id, service_chart_id, table, query, columns)

def getSalesMusicVideoQuery():
    table = 'bam_sales_music_video_daily'
    query = """
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

    columns = [
        'date_str', 'territory_id', 'add_drop',
        'previous_sales_position','position', 'artist_name', 'music_video_name',
        'isrc', 'sales_count', 'peak_ranking', 'peak_ranking_date', 'url'
    ]

    service_chart_id = 4
    service_id = 2
    return (service_id, service_chart_id, table, query, columns)

def getStreamingTrackAppleQuery():
    table = 'client_border_city_latest_table'
    query = """
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
            NULL as stream_count,
            peak_ranking,
            peak_ranking_date,
            url,
            label
        FROM {}
        WHERE service_id = 2
    """

    columns = [
        'date_str', 'territory_id', 'add_drop',
        'previous_track_position','chart_position', 'track_isrc', 'track_name',
        'artist_name', 'stream_count', 'peak_ranking',
        'peak_ranking_date', 'url', 'label'
    ]
    service_id = 2
    service_chart_id = 1

    return (service_id, service_chart_id, table, query, columns)

def getStreamingTrackSpotifyQuery():
    table = 'client_border_city_latest_table'
    query = """
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
            stream_count,
            peak_ranking,
            peak_ranking_date,
            url,
            label
        FROM {}
        WHERE service_id = 1
    """

    columns = [
        'date_str', 'territory_id', 'add_drop',
        'previous_track_position','chart_position', 'track_isrc', 'track_name',
        'artist_name', 'stream_count', 'peak_ranking',
        'peak_ranking_date', 'url', 'label'
    ]
    service_id = 1
    service_chart_id = 1

    return (service_id, service_chart_id, table, query, columns)

# Export files
def exportReport(service_id, service_chart_id, table, query, columns):
    report_date = getLatestDateFromTable(table, service_id)
    output_file = OUTPUT_FILE_TEMPLATE.format(CLIENT_NAME, report_date, "{0:0=3d}".format(service_chart_id), "{0:0=3d}".format(service_id) )

    # set stream_count to NULL if Apple charts for client (in DB, these values are INTs)
    #
    query = query.format(table)

    c.execute(query)
    rows = c.fetchall()

    with open(output_file, 'w') as f:
        writer = csv.writer(f)

        # write the header row
        writer.writerow(columns)

        # write all rows
        for row in rows:
            writer.writerow(row)

            #apple music track streaming report complete:
    print('{} {} report complete: {}'.format(SERVICE_MAP[service_id], SERVICE_CHARTS[service_chart_id], output_file))


def getAndPrintStreamingStats():
    # Returns a dict with key of service_id and the latest date for that service_id that's available
    # ex. latest_dates = {1: '2017-11-14', 2: '2017-11-15'}
    #
    c.execute("""
        SELECT service_id, date_str, count(date_str)
        FROM track_position
        GROUP BY service_id, date_str
        ORDER BY date_str DESC
		LIMIT 2
    """)

    latest_dates = {}

    rows = c.fetchmany(2)
    for row in rows:
        print('{} streaming: {} records on {}'.format(SERVICE_MAP[row[0]], row[2], row[1]))
        latest_dates[row[0]] = (row[1])

    return latest_dates

def getAndPrintSalesStats():
    # select media_type, the latest date, and the number of records for the latest date
    # fetching only three for the three media types
    # latest_dates = { 'music_video': '2017-11-21', 'track': , 'album': }
    c.execute("""
        SELECT media_type, date_str, count(date_str)
        FROM sales_position
        GROUP BY media_type, date_str
        ORDER BY date_str DESC
        LIMIT 3
    """)

    latest_dates = {}

    rows = c.fetchmany(3)
    for row in rows:
        print('iTunes {} sales: {} records on {}.'.format(row[0], row[2], row[1]))
        latest_dates[row[0]] = (row[1])
    return latest_dates

def printDivider(number):
    print('*' * number)

if __name__ == '__main__':
    conn = connect()
    c = conn.cursor()

    printDivider(40)

    # stats
    latest_streamingDates = getAndPrintStreamingStats()
    latest_salesDates = getAndPrintSalesStats()

    # ensure all sales dates are the same for reporting
    if latest_salesDates['album'] == latest_salesDates['track'] and latest_salesDates['album'] == latest_salesDates['music_video']:
        latest_date_for_salesService = latest_salesDates['album'] # if so, take one
    else:
        print('Check sales dates for albums {}, tracks {} and music videos {}'. format(latest_salesDates['album'], latest_salesDates['track'], latest_salesDates['music_video']))

    # # TODO: For user input of report generation
    #   -use sys.argv to get input from user on date, change latest_streamingDates
    #    -use if else condition on if sys.argv[1] for the date to process the reportexists
    # service_id = input('\nSelect latest report for service_id: 1 for Spotify, 2 for Apple: ')
    # report_type = input('\nSelect latest report - album, music_video or track:')
    # latest_date_for_service = latest_streamingDates[int(service_id)] # based on index
    # latest_date_for_salesService = latest_salesDates[report_type]
    # print('Generating report for {} on {}'.format(SERVICE_MAP[int(service_id)], latest_date_for_service))



    # timestamping
    printDivider(40)
    starttime_total = datetime.datetime.now()
    print('Starting processing at', starttime_total.strftime('%H:%M:%S %m-%d-%y'))
    printDivider(40)

    # # spotify streaming
    print('Generating Spotify stream reports')
    service_id = 1
    print(latest_streamingDates)
    generateStreamingReporting(service_id, latest_streamingDates[service_id])
    service_id, service_chart_id, table, query, columns = getStreamingTrackSpotifyQuery()
    exportReport(service_id, service_chart_id, table, query, columns)
    printDivider(40)

    # apple streaming
    # print('Generating Apple stream reports')
    # service_id = 2
    # generateStreamingReporting(service_id, latest_streamingDates[service_id])
    # service_id, service_chart_id, table, query, columns = getStreamingTrackAppleQuery()
    # exportReport(service_id, service_chart_id, table, query, columns)
    # printDivider(40)
    #
    # # iTunes sales
    # print('Generating iTunes sales reports')
    # service_id = 2
    # generateiTunesSalesReporting(service_id, latest_date_for_salesService) # itunes reporting views are all generated at the same time, thus need to use the same date
    # printDivider(40)
    #
    # service_id, service_chart_id, table, query, columns = getSalesTrackQuery()
    # exportReport(service_id, service_chart_id, table, query, columns)
    # printDivider(40)
    #
    # service_id, service_chart_id, table, query, columns = getSalesAlbumQuery()
    # exportReport(service_id, service_chart_id, table, query, columns)
    # printDivider(40)
    #
    # service_id, service_chart_id, table, query, columns = getSalesMusicVideoQuery()
    # exportReport(service_id, service_chart_id, table, query, columns)
    # printDivider(40)

    # timestamping
    endtime_total = datetime.datetime.now()
    processtime_total = endtime_total - starttime_total
    print('Ending processing at', endtime_total.strftime('%H:%M:%S %m-%d-%y'))
    print('Total processing time: %i minutes, %i seconds' % divmod(processtime_total.days *86400 + processtime_total.seconds, 60))