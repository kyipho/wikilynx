''' 
Processor for /query API endpoint

Sample API call:
https://7nv0a1b482.execute-api.us-east-2.amazonaws.com/test/query?query=select * from tbl_1a limit 10
'''

import sys
import json
import pymysql
import logging

from rds_config import endpoint, user0, user0_pw, database_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    connection = pymysql.connect(
        endpoint,
        user=user0,
        passwd=user0_pw,
        db=database_name
    )
except:
    logger.error('ERROR: Unexpected error: Could not connect to MySql instance.')
    sys.exit()

logger.info('SUCCESS: Connection to RDS mysql instance succeeded')


def decode(data):
    # decode if data is of type bytes
    return data.decode() if isinstance(data, bytes) else data


def lambda_handler(event, context):
    # extract SQL query passed by the user (user0 only has SELECT privileges)
    # queryStringParameters are pre-defined in AWS
    query = event['queryStringParameters']['query']

    # cursor to return results in a dictionary
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        # execute query
        cursor.execute(query)
        logger.info(f'\nQuery executed successfully:\n{query}\n')
    except:
        raise Exception(f'Query failed:\n{query}\n')

    # rows returned in a list of dictionaries (limit to 100)
    rows = cursor.fetchmany(100)
    
    # decode rows
    for i in range(len(rows)):
        rows[i] = {decode(k): decode(v) for k, v in rows[i].items()}

    # construct http response object
    response_object = {
        'headers': {'Content-Type': 'application/json'},
        'statusCode': 200,
        'body': json.dumps(rows, indent=2, default=str)
    }

    return response_object