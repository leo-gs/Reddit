from bs4 import BeautifulSoup
import json
import os
import praw
import prawcore
import psycopg2
from psycopg2 import extras as ext
import requests
import os
from time import sleep
import re


##########################
## Reddit API functions ##
##########################

all_accounts = {} ## Keep track of the profile data we get with each submission/comment
num_submissions = 1000
reddit_config_file = "reddit_config.txt"
db_config_file = "db_config.txt"
local_cache_dir = "/data/lgs17/reddit_apr2019/top_posts/"


def init_reddit():
    print("Creating Reddit object...", flush=True)
    reddit_config = {}
    with open(reddit_config_file) as f:
        for line in f.readlines():
            key, value = line.split("=")
            reddit_config[key.strip()] = value.strip()
        reddit = praw.Reddit(**reddit_config)
        return reddit


def get_subreddit(reddit, subreddit_name):
        return reddit.subreddit(subreddit_name)


## Pull all JSON serializable attributes into a JSON dictionary
def pull_json_from_obj(obj):
    obj_dir = vars(obj)
    json_dir = {}

    for key, value in obj_dir.items():
        try:
            json.dumps(value) ## Check if the value can be converted to JSON (will throw a TypeError if not)
            json_dir[key] = value

        except TypeError as ex:
            pass

    return json_dir


## Pull JSON data about post's author and cache it
def pull_author(author):
    if author:
        if author.name not in all_accounts:
            try:
                _ = author.created_utc ## To force the lazy object to load
                all_accounts[author.name] = pull_json_from_obj(author)

            except (prawcore.exceptions.NotFound, AttributeError) as ex:
                return

        return all_accounts[author.name]


## Get all comments from a submission by the submission_id
def pull_submission_comments(reddit, submission_id):
    submission = reddit.submission(id=submission_id)
    submission.comments.replace_more(limit=None)

    comments = submission.comments.list()

    comments_json = []
    for comment in comments:
        comment_json = pull_json_from_obj(comment)

        comment_author_json = pull_author(comment.author)
        comment_json["author"] = comment_author_json

        comments_json.append(comment_json)

    return comments_json


def scrape_subreddit_posts(subreddit, step, sub_endpoint, sub_type, subreddit_dir):
    get_current_queue_sql = """ SELECT subreddit FROM t1d_crosspost_queue """
    add_to_queue_sql = """ INSERT INTO t1d_crosspost_queue (subreddit, step) VALUES (%s, %s) """
    add_to_tie_table_sql = """ INSERT INTO t1d_crosspost_ties (target, label, source, post_type) VALUES (%s, %s, %s, %s)"""

    submissions = []

    for submission in sub_endpoint(limit=num_submissions):

        ## Get the top-level JSON from the submission
        submission_json = pull_json_from_obj(submission)
        submission_author_json = pull_author(submission.author)
        submission_json["author"] = submission_author_json

        ## Add the JSON for each comment
        comments_json = pull_submission_comments(reddit, submission_json["id"])
        submission_json["comments_extracted"] = comments_json

        if hasattr(submission, "crosspost_parent"):
            cp_author = {}
            try:
              crosspost_parent = reddit.submission(submission.crosspost_parent.split('_')[1])
              s = crosspost_parent.subreddit # force lazy load
              cp_author = pull_author(crosspost_parent.author)
            except prawcore.exceptions.Forbidden as ex:
              print(ex)
              print(submission.crosspost_parent)
            
            crosspost_json = pull_json_from_obj(crosspost_parent)
            crosspost_json["author"] = cp_author
            submission_json["crosspost_parent"] = crosspost_json

        submissions.append(submission_json)

    fname = "{}/{}.json".format(subreddit_dir, sub_type)
    with open(fname, "w+") as f:
        json.dump(submissions, f)
    
    print("dumped to {}".format(fname), flush=True)
    """
    crossposts = [submission for submission in submissions if "crosspost_parent" in submission]

    crosspost_ties = []
    crosspost_source_subreddits = set()
    for submission in crossposts:
        crosspost_source = submission["crosspost_parent"]["subreddit_name_prefixed"].replace("r/", "").lower()

        crosspost_source_subreddits.add(crosspost_source)
        crosspost_ties.append((subreddit.display_name, submission["id"], crosspost_source, sub_type))

    """
    if not submissions:
        return False

    """
    ## get current subreddits in queue
    current_queue = set(execute_in_db(get_current_queue_sql, return_first_only = True))

    ## see which source subreddits aren't already in the queue
    subreddits_to_add = crosspost_source_subreddits.difference(current_queue)

    ## upload them to the queue
    subreddit_rows = [(subreddit, step) for subreddit in subreddits_to_add]
    print("\t{} added to queue".format(len(subreddit_rows)))
    if subreddit_rows:
        execute_in_db(add_to_queue_sql, args = subreddit_rows, batch_insert = True)

    ## upload ties to the tie table
    print("\t{} ties uploaded".format(len(crosspost_ties)))
    if crosspost_ties:
        crosspost_ties = list(set(crosspost_ties))
        execute_in_db(add_to_tie_table_sql, args = crosspost_ties, batch_insert = True)
    """
    ## return if posts were successfully scraped (successful == there were posts to scrape)
    return True


########################
## Database functions ##
########################


## Get authenticated conn and cursor objects (returns conn, cursor tuple)
def get_db():
    with open(db_config_file) as f:
        conn_str = " ".join([l.strip() for l in f.readlines()])
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()
    return (conn, cursor)


## Execute single query and return results
def execute_in_db(query, return_results = False, return_first_only = False, args = None, batch_insert = False):
    conn, cursor = get_db()

    if args and batch_insert:
        ext.execute_batch(cursor, query, args)
    elif args:
        cursor.execute(query, args)
    else:
        cursor.execute(query)

    if return_first_only:
        results = [row[0] for row in cursor.fetchall()]
    elif return_results:
        results = cursor.fetchall()
    else:
        results = None

    conn.commit()

    cursor.close()
    conn.close()

    return results



######################
## Running snowball ##
######################

def check_num_unprocessed(queue_table):
    unprocessed_count_q = """ SELECT COUNT(*) FROM {} WHERE processed = 0 """.format(queue_table)

    unprocessed_count = execute_in_db(unprocessed_count_q, return_first_only = True)[0]

    return unprocessed_count


def crosspost_snowball(reddit, cache_dir):
    get_queue_sql = """ SELECT subreddit, step from t1d_crosspost_queue WHERE processed = 0 ORDER BY step DESC """

    mark_processed_sql = """ UPDATE t1d_crosspost_queue SET processed = 1 WHERE subreddit = '{}' """

    mark_unsuccessful_sql = """ UPDATE t1d_crosspost_queue SET processed = -1 WHERE subreddit = '{}' """

    queue = execute_in_db(get_queue_sql, return_results = True)
    print("{} subreddits to process".format(len(queue)))
    while queue:
        subreddit_name, step = queue.pop()
        print("Processing subreddit={}".format(subreddit_name), flush = True)

        subreddit_dir = "{}/{}".format(cache_dir, subreddit_name)
        if not os.path.isdir(subreddit_dir):
            os.mkdir(subreddit_dir)

        ## get the subreddit object from the name
        subreddit = get_subreddit(reddit, subreddit_name)
        ## try to pull each kind of post
        #new_successful = scrape_subreddit_posts(subreddit, step + 1, subreddit.new, "new", subreddit_dir)
        new_successful = True
        hot_successful = scrape_subreddit_posts(subreddit, step + 1, subreddit.hot, "hot", subreddit_dir)
        cont_successful = scrape_subreddit_posts(subreddit, step + 1, subreddit.controversial, "controversial", subreddit_dir)

        successful = (new_successful or hot_successful or cont_successful)
        if successful:
            execute_in_db(mark_processed_sql.format(subreddit_name))
        else:
            print("\tmarking subreddit={} unsuccessful".format(subreddit_name))
            execute_in_db(mark_unsuccessful_sql.format(subreddit_name))



## Get an authenticated Reddit API object
reddit = init_reddit()

# Scrape shared moderator edges
crosspost_snowball(reddit, cache_dir=local_cache_dir)

