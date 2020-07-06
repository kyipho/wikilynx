''' 
Processor for /category API endpoint

Sample API call:
https://7nv0a1b482.execute-api.us-east-2.amazonaws.com/test/category
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
    ''' Decode if data is of type bytes '''    
    return data.decode() if isinstance(data, bytes) else data


def lambda_handler(event, context):
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    query = 'SELECT * FROM tbl_2b WHERE TRUE'

    prep_statement_params = []

    if event['queryStringParameters'] is not None:
        # extract parameters passed via API
        api_params = event['queryStringParameters']

        # extend query and get params for SQL prepared statement
        for col in ['category_id', 'category_title', 'category_rank']:
            if col in api_params:
                query += f' AND {col} = %s'
                prep_statement_params.append(api_params[col])

    try:
        # execute query with prepared statement. return whole table if no params
        cursor.execute(query, prep_statement_params)
        logger.info(f'\nExecuting query:\n{cursor._last_executed}\n')
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