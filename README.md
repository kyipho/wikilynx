# Wikilynx

Creates a simple database with some Wikipedia dumps and processes the data.


# Tools
- Storage: Amazon Relational Database Service MYSQL (RDS MYSQL)
- API: AWS Lambda + AWS API Gateway
- Batch Job Automation: AWS Lambda + AWS Cloudwatch

These are part of the AWS free tier, do not require server management, and have good integration options with each other.


# Notes
- Created SQL tables are named according to the part of the question it answers
- Simplifications:
  - Reduced row counts for some intermediate SQL tables to reduce execution time (see setup.sql)
  - Tested batch job on a smaller wiki dump to reduce execution time (see batch_process.py)


# Questions

## 1a) Basic metadata for every wiki page
See `CREATE TABLE tbl_1a...` in `setup.sql`

## 1b) Links between wiki pages
See `CREATE TABLE tbl_1b...` in `setup.sql`

## 2a) API for an SQL query
See `api_query_processor.py`, which is run by AWS Lambda when called.

Sample call (in browser): 
`https://7nv0a1b482.execute-api.us-east-2.amazonaws.com/test/query?query=select * from tbl_1a limit 10`

## 2b) API for category + most outdated page
See `api_category_processor.py`, which is run by AWS Lambda when called.

Sample calls:
- Show top 10 categories and their most outdated pages (same results):
  - `https://7nv0a1b482.execute-api.us-east-2.amazonaws.com/test/category`
  - `https://7nv0a1b482.execute-api.us-east-2.amazonaws.com/test/query?query=select * from tbl_2b`
- Show data for a category within the top 10 based on ID:
  - `https://7nv0a1b482.execute-api.us-east-2.amazonaws.com/test/category?category_id=1478960`
- Show data for a category within the top 10 based on title:
  - `https://7nv0a1b482.execute-api.us-east-2.amazonaws.com/test/category?category_title=Articles_with_hCards`
- Show data for a category within the top 10 based on rank (1-10, 1 has most pages):
  - `https://7nv0a1b482.execute-api.us-east-2.amazonaws.com/test/category?category_rank=10`

## 3) Automation
See `batch_process.py`, which is run by AWS Lambda when called daily via an AWS Cloudwatch rule.

Instead of monthly, the pipeline runs daily to check for the last modified date of files in https://dumps.wikimedia.org/simplewiki/latest/, then compares with recorded insert dates in database. 

If dumps are of a later date, `batch_process.py` will download them, insert into the database, and rerun `setup.sql`.

Tested by inserting old versions (2020-02-01) of the dumps in the database, then rerunning just for the latest version (2020-07-01) of 'category' wikimedia dump table. See `screenshot_3` for updated `table_dates` SQL table.