import psycopg2
from psycopg2 import extras
import boto3
import json
import configparser
import os

if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['region_name']
    )
    sqs = aws_session.resource('sqs')

    queue = sqs.get_queue_by_name(QueueName='tweet-urls')
    queue.purge()

    cur.execute("SELECT * FROM query WHERE status = 'URL'")
    queries = cur.fetchall()
    for query in queries:
        cur.execute(
            """SELECT tweet.tweet_id,
              response->>'created_at' as created_at,
              json_data.value->>'expanded_url' AS url
            FROM tweet,
                  jsonb_array_elements(tweet.response#>'{{entities,urls}}') AS json_data
            WHERE tweet.query_alias = '{0}' and not exists(
                           select *
                           from tweet_url
                           where tweet_url.query_alias = '{0}' and
                                 tweet_url.tweet_id = tweet.tweet_id and 
                                 tweet_url.url = json_data.value->>'expanded_url')
        """.format(query['query_alias']))

        no_more_results = False
        while not no_more_results:
            tweets = cur.fetchmany(size=100)
            if len(tweets) == 0:
                no_more_results = True
            else:
                message_body = {
                    'query_alias': query['query_alias'],
                    'tweets': [{'tweet_id': tweet['tweet_id'],
                                'created_at': tweet['created_at'],
                                'url': tweet['url']} for tweet in tweets]
                }
                queue.send_message(MessageBody=json.dumps(message_body))

    conn.close()
