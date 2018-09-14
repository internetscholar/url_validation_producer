import psycopg2
from psycopg2 import extras
import boto3
import json
import configparser
import os
import logging


def main():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    logging.info('Connected to database %s', config['database']['db_name'])

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=config['aws']['region_queues']
    )
    sqs = aws_session.resource('sqs')
    logging.info('Connected to AWS in %s', aws_credential['region_name'])

    queue = sqs.get_queue_by_name(QueueName='url_validation')
    queue.purge()
    logging.info('Purged the queue url_validation, will execute SQL query')

    cur.execute("""
        (
          select distinct
            project.project_name,
            google_search_result.url
          from
            google_search_result,
            project,
            google_search_query
          where
            google_search_result.query_alias = google_search_query.query_alias and
            google_search_query.project_name = project.project_name and
            project.active and
            google_search_result.url is not null
          UNION
          select distinct
            project.project_name,
            jsonb_array_elements(twitter_hydrated_tweet.response->'entities'->'urls')->>'expanded_url' as url
          from
            twitter_hydrated_tweet,
            project
          where
            project.project_name = twitter_hydrated_tweet.project_name and
            project.active
        )
        EXCEPT
        (
          select
            url.project_name,
            url.url
          from
            url,
            project
          where
            project.project_name = url.project_name and
            project.active
          UNION
          select
            url_history.project_name,
            jsonb_array_elements(url_history.history)->>'url' as url
          from
            url_history,
            project
          where
            url_history.project_name = project.project_name and
            project.active and
            url_history.history is not null
        );
    """)
    logging.info('Done with SQL query')

    no_more_results = False
    while not no_more_results:
        urls = cur.fetchmany(size=100)
        if len(urls) == 0:
            no_more_results = True
            logging.info('No more results')
        else:
            queue.send_message(MessageBody=json.dumps(urls))
            logging.info('Enqueued message with %d urls', len(urls))

    conn.close()


if __name__ == '__main__':
    main()
