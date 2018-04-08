import sqlite3
DATABASE_NAME = 'v9sales.db'

# sqlite database filename/path
DATABASE_FILE = '../{}'.format(DATABASE_NAME)

class TrackDatabase(object):
    """ SQLite Database Manager """
    def __init__(self, db_file=DATABASE_NAME):
        super(TrackDatabase, self).__init__()
        self.db_file = db_file
        self.init_database()
        # self.seed_data()

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
                release_date text NOT NULL,
                isrc text NOT NULL,
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

        self.c.execute('''
            CREATE TABLE IF NOT EXISTS peak_sales_position (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                territory_id integer NOT NULL,
                media_id integer NOT NULL,
                media_type text NOT NULL,
                first_added text NOT NULL,
                last_seen text NOT NULL,
                peak_rank integer NOT NULL,
                peak_date text NOT NULL,
                UNIQUE(service_id, territory_id, media_id, media_type) ON CONFLICT IGNORE,
                FOREIGN KEY (service_id) REFERENCES service(id),
                FOREIGN KEY (territory_id) REFERENCES territory(id)
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

        self.c.execute('''
            CREATE TABLE IF NOT EXISTS sales_position (
                id integer PRIMARY KEY,
                service_id integer NOT NULL,
                territory_id integer NOT NULL,
                media_id integer NOT NULL,
                media_type text NOT NULL,
                position integer NOT NULL,
                sales_count integer NOT NULL DEFAULT 0,
                date_str text NOT NULL,
                UNIQUE(service_id, territory_id, media_id, media_type, date_str) ON CONFLICT IGNORE,
                FOREIGN KEY (service_id) REFERENCES service(id),
                FOREIGN KEY (territory_id) REFERENCES territory(id)
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

    def seed_data(self):
        # currently just a string placeholder of sql statements to execute in the db
        # to automate, create a list of strings and c.execute each one individually
        '''
            INSERT INTO `territory` VALUES (1,'global','global');
            INSERT INTO `territory` VALUES (2,'us','United States');
            INSERT INTO `territory` VALUES (3,'gb','United Kingdom');
            INSERT INTO `territory` VALUES (4,'ad','Andorra');
            INSERT INTO `territory` VALUES (5,'ar','Argentina');
            INSERT INTO `territory` VALUES (6,'at','Austria');
            INSERT INTO `territory` VALUES (7,'au','Australia');
            INSERT INTO `territory` VALUES (8,'be','Belgium');
            INSERT INTO `territory` VALUES (9,'bg','Bulgaria');
            INSERT INTO `territory` VALUES (10,'bo','Bolivia');
            INSERT INTO `territory` VALUES (11,'br','Brazil');
            INSERT INTO `territory` VALUES (12,'ca','Canada');
            INSERT INTO `territory` VALUES (13,'ch','Switzerland');
            INSERT INTO `territory` VALUES (14,'cl','Chile');
            INSERT INTO `territory` VALUES (15,'co','Colombia');
            INSERT INTO `territory` VALUES (16,'cr','Costa Rica');
            INSERT INTO `territory` VALUES (17,'cy','Cyprus');
            INSERT INTO `territory` VALUES (18,'cz','Czech Republic');
            INSERT INTO `territory` VALUES (19,'de','Germany');
            INSERT INTO `territory` VALUES (20,'dk','Denmark');
            INSERT INTO `territory` VALUES (21,'do','Dominican Republic');
            INSERT INTO `territory` VALUES (22,'ec','Ecuador');
            INSERT INTO `territory` VALUES (23,'ee','Estonia');
            INSERT INTO `territory` VALUES (24,'es','Spain');
            INSERT INTO `territory` VALUES (25,'fi','Finland');
            INSERT INTO `territory` VALUES (26,'fr','France');
            INSERT INTO `territory` VALUES (27,'gr','Greece');
            INSERT INTO `territory` VALUES (28,'gt','Guatemala');
            INSERT INTO `territory` VALUES (29,'hk','Hong Kong');
            INSERT INTO `territory` VALUES (30,'hn','Honduras');
            INSERT INTO `territory` VALUES (31,'hu','Hungary');
            INSERT INTO `territory` VALUES (32,'id','Indonesia');
            INSERT INTO `territory` VALUES (33,'ie','Ireland');
            INSERT INTO `territory` VALUES (34,'is','Iceland');
            INSERT INTO `territory` VALUES (35,'it','Italy');
            INSERT INTO `territory` VALUES (36,'jp','Japan');
            INSERT INTO `territory` VALUES (37,'lt','Lithuania');
            INSERT INTO `territory` VALUES (38,'lu','Luxembourg');
            INSERT INTO `territory` VALUES (39,'lv','Latvia');
            INSERT INTO `territory` VALUES (40,'mc','Monaco');
            INSERT INTO `territory` VALUES (41,'mt','Malta');
            INSERT INTO `territory` VALUES (42,'mx','Mexico');
            INSERT INTO `territory` VALUES (43,'my','Malaysia');
            INSERT INTO `territory` VALUES (44,'ni','Nicaragua');
            INSERT INTO `territory` VALUES (45,'nl','Netherlands');
            INSERT INTO `territory` VALUES (46,'no','Norway');
            INSERT INTO `territory` VALUES (47,'nz','New Zealand');
            INSERT INTO `territory` VALUES (48,'pa','Panama');
            INSERT INTO `territory` VALUES (49,'pe','Peru');
            INSERT INTO `territory` VALUES (50,'ph','Philippines');
            INSERT INTO `territory` VALUES (51,'pl','Poland');
            INSERT INTO `territory` VALUES (52,'pt','Portugal');
            INSERT INTO `territory` VALUES (53,'py','Paraguay');
            INSERT INTO `territory` VALUES (54,'se','Sweden');
            INSERT INTO `territory` VALUES (55,'sg','Singapore');
            INSERT INTO `territory` VALUES (56,'sk','Slovakia');
            INSERT INTO `territory` VALUES (57,'sv','El Salvador');
            INSERT INTO `territory` VALUES (58,'th','Thailand');
            INSERT INTO `territory` VALUES (59,'tr','Turkey');
            INSERT INTO `territory` VALUES (60,'tw','Taiwan');
            INSERT INTO `territory` VALUES (61,'uy','Uruguay');
            INSERT INTO `territory` VALUES (62,'ae','UAE');
            INSERT INTO `territory` VALUES (63,'ag','Antigua and Barbuda');
            INSERT INTO `territory` VALUES (64,'ai','Anguilla');
            INSERT INTO `territory` VALUES (65,'al','Albania');
            INSERT INTO `territory` VALUES (66,'am','Armenia');
            INSERT INTO `territory` VALUES (67,'ao','Angola');
            INSERT INTO `territory` VALUES (68,'az','Azerbaijan');
            INSERT INTO `territory` VALUES (69,'bb','Barbados');
            INSERT INTO `territory` VALUES (70,'bf','Burkina Faso');
            INSERT INTO `territory` VALUES (71,'bh','Bahrain');
            INSERT INTO `territory` VALUES (72,'bj','Benin');
            INSERT INTO `territory` VALUES (73,'bm','Bermuda');
            INSERT INTO `territory` VALUES (74,'bn','Brunei Darussalam');
            INSERT INTO `territory` VALUES (75,'bs','Bahamas');
            INSERT INTO `territory` VALUES (76,'bt','Bhutan');
            INSERT INTO `territory` VALUES (77,'bw','Botswana');
            INSERT INTO `territory` VALUES (78,'by','Belarus');
            INSERT INTO `territory` VALUES (79,'bz','Belize');
            INSERT INTO `territory` VALUES (80,'cg','Congo, Republic of the');
            INSERT INTO `territory` VALUES (81,'cn','China');
            INSERT INTO `territory` VALUES (82,'cv','Cape Verde');
            INSERT INTO `territory` VALUES (83,'dm','Dominica');
            INSERT INTO `territory` VALUES (84,'dz','Algeria');
            INSERT INTO `territory` VALUES (85,'eg','Egypt');
            INSERT INTO `territory` VALUES (86,'fj','Fiji');
            INSERT INTO `territory` VALUES (87,'fm','Micronesia, Federated States of');
            INSERT INTO `territory` VALUES (88,'gd','Grenada');
            INSERT INTO `territory` VALUES (89,'gh','Ghana');
            INSERT INTO `territory` VALUES (90,'gm','Gambia');
            INSERT INTO `territory` VALUES (91,'gw','Guinea-Bissau');
            INSERT INTO `territory` VALUES (92,'gy','Guyana');
            INSERT INTO `territory` VALUES (93,'hr','Croatia');
            INSERT INTO `territory` VALUES (94,'il','Israel');
            INSERT INTO `territory` VALUES (95,'in','India');
            INSERT INTO `territory` VALUES (96,'jm','Jamaica');
            INSERT INTO `territory` VALUES (97,'jo','Jordan');
            INSERT INTO `territory` VALUES (98,'ke','Kenya');
            INSERT INTO `territory` VALUES (99,'kg','Kyrgyzstan');
            INSERT INTO `territory` VALUES (100,'kh','Cambodia');
            INSERT INTO `territory` VALUES (101,'kn','St. Kitts and Nevis');
            INSERT INTO `territory` VALUES (102,'kr','Korea, Republic of');
            INSERT INTO `territory` VALUES (103,'kw','Kuwait');
            INSERT INTO `territory` VALUES (104,'ky','Cayman Islands');
            INSERT INTO `territory` VALUES (105,'kz','Kazakhstan');
            INSERT INTO `territory` VALUES (106,'la','Lao People''s Democratic Republic');
            INSERT INTO `territory` VALUES (107,'lb','Lebanon');
            INSERT INTO `territory` VALUES (108,'lc','St. Lucia');
            INSERT INTO `territory` VALUES (109,'lk','Sri Lanka');
            INSERT INTO `territory` VALUES (110,'lr','Liberia');
            INSERT INTO `territory` VALUES (111,'md','Moldova');
            INSERT INTO `territory` VALUES (112,'mg','Madagascar');
            INSERT INTO `territory` VALUES (113,'mk','Macedonia');
            INSERT INTO `territory` VALUES (114,'ml','Mali');
            INSERT INTO `territory` VALUES (115,'mn','Mongolia');
            INSERT INTO `territory` VALUES (116,'mo','Macau');
            INSERT INTO `territory` VALUES (117,'mr','Mauritania');
            INSERT INTO `territory` VALUES (118,'ms','Montserrat');
            INSERT INTO `territory` VALUES (119,'mu','Mauritius');
            INSERT INTO `territory` VALUES (120,'mw','Malawi');
            INSERT INTO `territory` VALUES (121,'mz','Mozambique');
            INSERT INTO `territory` VALUES (122,'na','Namibia');
            INSERT INTO `territory` VALUES (123,'ne','Niger');
            INSERT INTO `territory` VALUES (124,'ng','Nigeria');
            INSERT INTO `territory` VALUES (125,'np','Nepal');
            INSERT INTO `territory` VALUES (126,'om','Oman');
            INSERT INTO `territory` VALUES (127,'pg','Papua New Guinea');
            INSERT INTO `territory` VALUES (128,'pk','Pakistan');
            INSERT INTO `territory` VALUES (129,'pw','Palau');
            INSERT INTO `territory` VALUES (130,'qa','Qatar');
            INSERT INTO `territory` VALUES (131,'ro','Romania');
            INSERT INTO `territory` VALUES (132,'ru','Russia');
            INSERT INTO `territory` VALUES (133,'sa','Saudi Arabia');
            INSERT INTO `territory` VALUES (134,'sb','Solomon Islands');
            INSERT INTO `territory` VALUES (135,'sc','Seychelles');
            INSERT INTO `territory` VALUES (136,'si','Slovenia');
            INSERT INTO `territory` VALUES (137,'sl','Sierra Leone');
            INSERT INTO `territory` VALUES (138,'sn','Senegal');
            INSERT INTO `territory` VALUES (139,'sr','Suriname');
            INSERT INTO `territory` VALUES (140,'st','São Tomé and Príncipe');
            INSERT INTO `territory` VALUES (141,'sz','Swaziland');
            INSERT INTO `territory` VALUES (142,'tc','Turks and Caicos');
            INSERT INTO `territory` VALUES (143,'td','Chad');
            INSERT INTO `territory` VALUES (144,'tj','Tajikistan');
            INSERT INTO `territory` VALUES (145,'tm','Turkmenistan');
            INSERT INTO `territory` VALUES (146,'tn','Tunisia');
            INSERT INTO `territory` VALUES (147,'tt','Trinidad and Tobago');
            INSERT INTO `territory` VALUES (148,'tz','Tanzania');
            INSERT INTO `territory` VALUES (149,'ua','Ukraine');
            INSERT INTO `territory` VALUES (150,'ug','Uganda');
            INSERT INTO `territory` VALUES (151,'uz','Uzbekistan');
            INSERT INTO `territory` VALUES (152,'vc','St. Vincent and The Grenadines');
            INSERT INTO `territory` VALUES (153,'ve','Venezuela');
            INSERT INTO `territory` VALUES (154,'vg','British Virgin Islands');
            INSERT INTO `territory` VALUES (155,'vn','Vietnam');
            INSERT INTO `territory` VALUES (156,'ye','Yemen');
            INSERT INTO `territory` VALUES (157,'za','South Africa');
            INSERT INTO `territory` VALUES (158,'zw','Zimbabwe');
            INSERT INTO `service` VALUES (1,'Spotify');
            INSERT INTO `service` VALUES (2,'Apple');
        '''
if __name__ == '__main__':
    db = TrackDatabase(DATABASE_FILE)
