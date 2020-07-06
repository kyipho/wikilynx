'''
To be run daily via AWS Cloudwatch as an AWS Lambda function.

Checks https://dumps.wikimedia.org/simplewiki/latest/ 
for latest uploads for the following sql files:
    - page
    - pagelinks
    - category
    - categorylinks

e.g. simplewiki-latest-pagelinks.sql.gz

Then check latest refresh date for these files in db under 'table_dates'.

If updated, drop old versions in db and create these instead,
then rerun queries defined in setup.sql
'''

import logging
import requests
import os
import gzip
import pymysql
from pymysql.constants import CLIENT
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
from rds_config import endpoint, admin, admin_pw, database_name

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(message)s')

# only looking for latest available uploads
base_url = 'https://dumps.wikimedia.org/simplewiki/latest/'

# file name format
file_name = 'simplewiki-latest-{table}.sql.gz'

# names of created tables after running downloaded sql files
tables = ['page', 'pagelinks', 'category', 'categorylinks']

# dict of file names to the corresponding tables
fn_to_t = {file_name.format(table=t): t for t in tables}


def get_table_dates_on_wm():
    '''
    Scrapes https://dumps.wikimedia.org/simplewiki/latest/
    to check latest refresh date for these tables.

    Returns a dict of {table: last refresh date}
    '''
    try:
        logger.info('\nSending request to wikimedia...\n')

        index = requests.get(base_url).text

        logger.info('\nSUCCESS: Got response from wikimedia\n')
    except:
        logger.error('\nERROR: Could not get response from wikimedia\n')
        raise

    soup = bs(index, 'html.parser')

    table_to_date = {}

    for fn in fn_to_t:
        # expects first 11 chars of link.next_sibling to be a date
        # e.g. 05-Jul-2020
        last_mod_str = soup.find('a', text=fn).next_sibling.strip()[:11]
        last_mod_date = dt.strptime(last_mod_str, '%d-%b-%Y').date()

        # use table name as key
        table_to_date[fn_to_t[fn]] = last_mod_date

    return table_to_date


def get_table_dates_in_db():
    '''
    Uses /query API endpoint to extract last refresh date for these tables from db.

    Returns a dict of {table: last refresh date}
    '''
    endpoint = 'https://7nv0a1b482.execute-api.us-east-2.amazonaws.com/test/query?query='
    query = 'SELECT * FROM table_dates'

    try:
        logger.info('\nSending request to database API...\n')

        # API returns a list of dictionaries:
        # [{'table_name': 'page', 'date_inserted': date('2020-02-01')}, ...]
        res = requests.get(endpoint + query).json()

        logger.info('\nSUCCESS: Got response from database API\n')
    except:
        logger.error('\nERROR: Could not get response from database API\n')
        raise

    table_to_date = {
        r['table_name']: dt.strptime(r['date_inserted'], '%Y-%m-%d').date()
        for r in res
    }

    return table_to_date


wm = get_table_dates_on_wm()
db = get_table_dates_in_db()

logger.info(f'\nLast refresh dates in wikimedia:\n{wm}')
logger.info(f'\nLast refresh dates in database:\n{db}')

# if wikimedia date > database date, that table needs updating in db
needs_update = {t: wm[t] > db[t] for t in tables}
logger.info(f'\nTables with True need an update:\n{needs_update}')


def download_files():
    ''' Downloads files from wikimedia if needed based on needs_update. '''
    downloads = []

    for fn, t in fn_to_t.items():
        if needs_update[t]:
            download_path = base_url + fn

            try:
                logger.info(f'\nAttempting to download {fn}...\n')

                file = requests.get(download_path, allow_redirects=True)
                open(f'{fn}', 'wb').write(file.content)
                downloads.append(fn)

                logger.info(f'\nSUCCESS: Downloaded {fn}\n')
            except:
                logger.error(f'\nERROR: {fn} could not be downloaded\n')
                raise
    
    return downloads


downloads = download_files()

# connect to database if there are newly downloaded files
if downloads:
    try:
        connection = pymysql.connect(
            endpoint,
            user=admin,
            passwd=admin_pw,
            db=database_name,
            # allow running of multiple sql statements in same query
            client_flag=CLIENT.MULTI_STATEMENTS
        )
    except:
        logger.error('\nERROR: Unexpected error: Could not connect to MySql instance.\n')
        raise

    logger.info('\nSUCCESS: Connection to RDS mysql instance succeeded\n')


def lambda_handler(event, context):
    '''
    Executes SQL files that have been downloaded, then run setup.sql.

    Raises exception if any part of the process fails.
    '''
    cursor = connection.cursor()

    # for simplicity, overwrite old data in table_dates
    # ideally, put in dimension table (e.g. dim_table_dates) and add new rows
    table_dates_query = '''
        UPDATE table_dates
        SET date_inserted = DATE('{last_modified}')
        WHERE table_name = '{table}'
    '''

    for dl in downloads:
        query = ''

        # open file, decode lines and append to query 
        with gzip.open(dl, 'r') as reader:
            for line in reader:
                line_str = line.decode('utf-8')
                query += line_str

        # execute queries in file
        try:
            logger.info(f'\nAttempting to run {dl}...\n')
            cursor.execute(query)
            logger.info(f'\nSUCCESS: Ran {dl} \n')

            # update data in table_dates
            try:
                logger.info(f'\nAttempting to update table_dates...\n')
                cursor.execute(
                    table_dates_query.format(
                        last_modified=wm[fn_to_t[dl]],
                        table=fn_to_t[dl]
                    )
                )
                logger.info(f'\nSUCCESS: Updated table_dates \n')
            except:
                logger.error(f'\nERROR: Could not update table_dates\n')
                raise

        except:
            logger.error(f'\nERROR: Could not execute {dl}\n')
            raise

        # delete downloaded file
        try:
            logger.info(f'\nAttempting to delete {dl}...\n')
            os.remove(dl)
            logger.info(f'\nSUCCESS: Deleted {dl}\n')
        except:
            logger.error(f'\nERROR: {dl} could not be deleted\n')
            raise

    # once all files have been executed, run setup.sql to update downstream tables
    try:
        with open('setup.sql', 'r') as reader:
            setup = ''.join(reader.readlines())

        logger.info(f'\nAttempting to run setup.sql...\n')
        cursor.execute(setup)
        logger.info(f'\nSUCCESS: Ran setup.sql\n')
    except:
        logger.error(f'\nERROR: {dl} could not be deleted\n')
        raise

    # commit changes to database if no exceptions raised
    connection.commit()

