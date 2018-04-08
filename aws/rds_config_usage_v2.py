import psycopg2
import sys
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

rds_host  = "beats.cu8wph61yh7y.us-west-1.rds.amazonaws.com"
name = "beatsdj"
password = "beatsdj123"
db_name = "beats"

try:
    conn = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name)
    conn.autocommit = True
except:
    sys.exit()

logger.info("SUCCESS: Connection to RDS posgresql instance succeeded")

def handler(event, context):
    """
    This function fetches content from mysql RDS instance
    """

    item_count = 0

    with conn.cursor() as cur:
        # cur.execute("create table employee ( id int NOT NULL, name varchar(255) NOT NULL, PRIMARY KEY (id))")
        cur.execute("insert into employee (id, name) values(1, 'Hey')")
        cur.execute("insert into employee (id, name) values(2, 'Go')")
        cur.execute("insert into employee (id, name) values(3, 'wobbly')")

        for row in cur:
            item_count += 1
            logger.info(row)


    return "Added {} items from RDS PostgreSQL table".format(item_count)


# handler({}, 'go')
