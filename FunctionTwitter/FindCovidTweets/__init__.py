from datetime import datetime, date, timedelta
import os
import csv
import time
import json
import tweepy
import logging
import tempfile
import traceback
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from requests.exceptions import Timeout

def main(dailytrigger: func.TimerRequest, loggerBlob: func.Out[func.InputStream]):
    # Logging
    logging.info("Configuring logger...")
    logger = ""

    def log(text, error = False, save = False):
        nonlocal logger, loggerBlob
        logger+=f"{text} - {datetime.now()}\n"
        if error:
            logging.error(text)
        else: logging.info(text)
        if save: # we upload to a log file because Azure's App Insights is inconsistent
            loggerBlob.set(logger)

    # Options
    log("Formatting parameters...")

    QUERY = "#covid19 -filter:retweets"
    SAVE_INTERVAL = 5000
    MAX_RETRIES = 3
    MAX_TWEETS = -1
    REQUEST_WAIT_TIME = 0.55
    LOG_INTERVAL = 5000
    LAST_TWEET_ID = None
    TIMEOUT_WAIT_TIME = 300
    DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d").split('-')

    CONTAINER_TEMP_NAME = "twitterdatatemp"
    CONTAINER_FINAL_NAME = "twitterdataraw"

    YEAR = int(DATE[0])
    MONTH = int(DATE[1])
    DAY = int(DATE[2])

    # Azure
    log("Configuring Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(os.environ["STORAGE_CONNECTION_STRING"])
    output_blob = blob_service.get_blob_client(CONTAINER_TEMP_NAME, f"{YEAR}-{MONTH}-{DAY}.csv")
    if output_blob.exists():
        output_blob.delete_blob()
    output_blob.create_append_blob()

    final_blob = blob_service.get_blob_client(CONTAINER_FINAL_NAME, f"{YEAR}-{MONTH}-{DAY}.csv")

    # API Setup
    log("Setting up API...")
    auth = tweepy.OAuthHandler(os.environ["CONSUMER_API_KEY"], os.environ["CONSUMER_API_SECRET"])
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # Logic
    def throttle(cursor):
        nonlocal REQUEST_WAIT_TIME
        while True:
            time.sleep(REQUEST_WAIT_TIME)
            yield cursor.next()

    def save_tweets():
        nonlocal tweets, output_blob
        output_blob.append_block(tweets.encode())
        tweets = ""

    tweets = ""
    def loop(last_tweet_id, count = 0, retry_count = 0):
        nonlocal QUERY, LOG_INTERVAL, SAVE_INTERVAL, MAX_RETRIES, TIMEOUT_WAIT_TIME, tweets, logger

        log(f"Started new loop", save=True)
        pages = throttle(tweepy.Cursor(api.search, 
                                    q=QUERY,
                                    until=datetime(YEAR, MONTH, DAY + 1).date().strftime('%Y-%m-%d'), # this bugs out the last day of each month...
                                    include_entities=False,
                                    count=100,
                                    max_id=last_tweet_id).pages())
        log("Created query item successfully")

        insights_log_count = 0
        save_count = 1
        try:
            for page in pages:
                for tweet in page:
                    if tweet.created_at.date() < datetime(YEAR, MONTH, DAY).date():
                        log(f"Found last tweet from the specified date, stopping script - {count} tweets")
                        save_tweets()
                        return
                    if MAX_TWEETS > 0 and count > MAX_TWEETS:
                        log(f"Reached max amount of tweets, stopping script - {count} tweets")
                        save_tweets()
                        return
                    if count >= SAVE_INTERVAL * save_count:
                        log(f"Reached save interval of {count} tweets, saving...")
                        save_tweets()
                        save_count += 1

                    tweet_text = tweet.text.replace('\n', '\\n').replace('"', "'")
                    tweets += f'{tweet.id},"{tweet_text}",{tweet.created_at}\n'
                    count += 1
                    last_tweet_id = tweet.id
                # upload logs every X interval, because Azure's App Insights is inconsistent
                save = False
                if count / LOG_INTERVAL >= insights_log_count:
                    save = True
                    insights_log_count += 1
                log(f"Found page with {len(page)} items - total: {count} - last tweet id: {last_tweet_id}", save=save)
        except:
            if retry_count >= MAX_RETRIES:
                log(f"Max retry amount of {MAX_RETRIES} reached, raising error...")
                raise
            log(traceback.format_exc(), error=True)
            log(f"Error thrown - {retry_count} retries - count: {count} - id of last tweet: {last_tweet_id}", error=True)
            log(f"Waiting {TIMEOUT_WAIT_TIME} seconds and retrying...", save=True)
            time.sleep(TIMEOUT_WAIT_TIME)
            log(f"Waited {TIMEOUT_WAIT_TIME} seconds, re-starting loop")
            return loop(last_tweet_id, count, retry_count+1)

    log(f"Started script")
    try:
        loop(LAST_TWEET_ID)
        if len(tweets) > 0:
            save_tweets()
        log("Done gathering tweets, moving from temp storage to final storage...")
        temp_blob_url = f"https://{os.environ["STORAGE_ACCOUNT_NAME"]}.blob.core.windows.net/{CONTAINER_TEMP_NAME}/{YEAR}-{MONTH}-{DAY}.csv"
        log(f"Copying from url: {temp_blob_url}")
        final_blob.start_copy_from_url(temp_blob_url)
        log("Deleting temp blob...")
        output_blob.delete_blob()
        loggerBlob.set(logger)
    except:
        log("Unhandled error, didn't move file to final storage.", error=True)
        loggerBlob.set(logger)
        raise
