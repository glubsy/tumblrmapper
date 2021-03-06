#!/usr/bin/env python3.6
import argparse
import configparser
import json
import logging
import os
import queue
import random
import signal
import sys
import threading
import time
import traceback
from concurrent import futures
from itertools import cycle
from logging.handlers import RotatingFileHandler
import requests
import db_handler
import api_keys
import instances
import archive_lists
import hashes
from proxies import ProxyScanner
from constants import BColors

# try:
#     from tqdm import tqdm
#     TQDM_AVAILABLE = True
# except ImportError:
#     TQDM_AVAILABLE = False
# import curses
# import ratelimit

SCRIPTDIR = os.path.dirname(__file__)
THREADS = 5
asked_termination = False

def parse_args():
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description='tumblrmapper tumblr url mapper.')
    parser.add_argument('-c', '--config_path', action="store",
                        help="Path to config directory.")
    parser.add_argument('-d', '--data_path', action="store", default=None,
                        help="Set default path to data directory, where logs and DB are stored")
    parser.add_argument('-l', '--log_level', action="store", default="warning",
                        help="Set log level: DEBUG, INFO, WARNING (default), ERROR, CRITICAL")

    # actiongrp = parser.add_mutually_exclusive_group()
    parser.add_argument('-u', '--create_archive_list', action="store_true",
                    help="Recreate archive file listing.")

    parser.add_argument('-n', '--create_blank_db', action="store_true",
                        help="Create a blank DB in data_dir and populate it")
    parser.add_argument('-s', '--update_archives', action="store_true",
                    help="Populate DB with archives")
    parser.add_argument('-b', '--update_blogs', action="store_true",
                    help="Populate DB with blogs")
    parser.add_argument('-f', '--ignore_duplicates', action="store_true", default=False,
                help="Ignore duplicate posts, keep scraping away")
    parser.add_argument('-m', '--record_context', action="store_true", default=False,
                help="Do not skip recording context of posts in DB.")
    parser.add_argument('-t', '--ignore_non_tumblr_urls', action="store_true", default=False,
                help="Do not record URLS if they do not hold 'tumblr' in them.")
    parser.add_argument('-i', '--scrape_notes', action="store", default=None,
                help="Try to populate BLOGS table with new blogs: all\
blogs which have appeared in the notes of posts that belonged to either dead blogs (reblogs) \
or blogs sorted by priority (regular posts). value=[dead|priority]")

    parser.add_argument('-q', '--compute_hashes', action="store_true", default=False,
                help="For each blog, compute hashes based on filenames belonging \
to respective posts, and add them to the hashes columns in DB.")
    parser.add_argument('-z', '--match_hashes', action="store_true", default=False,
                help="Group all lost file basenames by their blog hashes and\
attempts to match them to known hashes in hash column in DB")

    parser.add_argument('-w', '--reset_blogs_by_hash', action="store_true", default=False,
                help="reset status for blogs for which we have found their corresponding\
hashes in the hashes module, listed in the json. Then re-scrape them deep to add\
all blogs found in notes from the reblogs.")

    parser.add_argument('-g', '--deep_scrape', action="store_true", default=False,
                help="scrape notes and reblogs info from blogs by priority, add\
blogs found in notes and reblogs to gathered blogs in BLOGS table.")


    parser.add_argument('-p', '--proxies', action="store_true", default=False,
                        help="Use randomly selected proxies")
    parser.add_argument('-v', '--api_version', action="store", type=int, default=2,
                        help="API version to query, default is 2.")



    return parser.parse_args()


def parse_config(config_path=SCRIPTDIR, data_path=None):
    """Configuration derived from defaults & file."""

    config_path = SCRIPTDIR + os.sep + "config" #FIXME: hardcoded configpath
    if data_path is None:
        data_path = SCRIPTDIR

    # Set some config defaults
    config_defaults = { "config_path": config_path,
                        "data_path": data_path + os.sep,
                        "blogs_to_scrape": config_path + "blogs_to_scrape.txt", #initial blog list to populate DB
                        "archives": config_path, #initial blog list to populate DB
                        "log_path": data_path + os.sep + "scraper.log", #log for downloads and proxies
                        "db_filepath": data_path + os.sep, #blank initial DB file
                        "db_filename": 'tumblrmapper.fdb', #blank initial DB file
                        "api_version": "2", #use api v2 by default
                        "use_proxies": False, #use random proxies, or not
                        "api_keys": data_path + os.sep + "api_keys.json",
                        "proxies": data_path + os.sep + "proxies.json",
                        "threads": 5,
                        "nice_level": 10,
                        "log_level": "WARNING"
                        }

    config = configparser.SafeConfigParser(config_defaults)
    config.add_section('tumblrmapper')

    # Try to read config file (either passed in, or default value)
    # conf_file = os.path.join(config.get('tumblrmapper', 'config_path'), 'config')
    logging.debug(f"Trying to read config file: {config_path}")
    result = config.read(config_path)
    if not result:
        logging.warning(f"Unable to read config file: {config_path}")

    config.set('tumblrmapper', 'blogs_to_scrape', \
    SCRIPTDIR + os.sep + config.get('tumblrmapper', 'blogs_to_scrape'))

    config.set('tumblrmapper', 'archives', \
    SCRIPTDIR + os.sep + config.get('tumblrmapper', 'archives'))

    config.set('tumblrmapper', 'db_filepath', \
    os.path.expanduser(config.get('tumblrmapper', 'db_filepath')))

    logging.info(f"Merged config: {sorted(dict(config.items('tumblrmapper')).items())}")

    return config



def input_thread(event, worker_threads):
    """ daemon threads setting pill2kill event when keyboard press occurs"""
    # DEBUG
    # attempt = 0
    # while attempt < 10:
    #     attempt += 1
    #     print(BColors.BOLD + "Length of worker threads list:\
    #  {0}, alive: {1}, list: {2}, current {3}"\
    #     .format(len(worker_threads), threading.active_count(), \
    # threading.enumerate(), threading.current_thread()) + BColors.ENDC)
    #     for thread in threading.enumerate():
    #         print(BColors.BOLD + "thread: {0} daemon: {1}"\
    # .format(thread, thread.daemon) + BColors.ENDC)
    #     print("\n")
    #     time.sleep(1)

    while not event.is_set():
        if threading.active_count() < 3:
            logging.debug(BColors.LIGHTYELLOW + \
            "Less than 3 threads, exiting." + BColors.ENDC)
            event.set()
            return
        if input():
            logging.critical("Received keyboard input, stopping threads")
            event.set()

    # while True:
    #     if input():
    #         print(qlist)
    #         qlist.append(None)


def process_notes(db, lock, db_update_lock, pill2kill, priority):
    """Fetch dead or wiped blog from BLOGS table in DB
    for each reblog of that blog, request the post through the REST API
    and add all blogs found in the notes to our BLOGS table for later scraping"""

    deep_scrape = {}
    if instances.my_args.deep_scrape:  # should always be True here!
        deep_scrape['notes_info'] = True
        deep_scrape['reblog_info'] = True

    con = db.connect()
    while not pill2kill.is_set():

        requester = Requester(pill2kill=pill2kill, lock=lock)
        update = UpdatePayload()
        blog_404_cache = set()

        while not pill2kill.is_set():
            with db_update_lock:
                posts_rows = db_handler.fetch_all_blog_s_posts(db, con, priority=priority)
                if len(posts_rows) == 0:
                    # we're most likely done
                    break
                if posts_rows[0][0] == 0: # o_post_id
                    # this one had nothing :(
                    logging.warning(f"{BColors.YELLOW}Blog \
'{posts_rows[0][-1]}' found no reblog (with no notes) in DB.{BColors.ENDC}")
                    continue
                break

        if not posts_rows:
            logging.warning(f'{BColors.GREEN}{BColors.BLINKING}\
Done fetching reblogs and populating blogs from notes.{BColors.ENDC}')
            break

        targeted_dead_blog = posts_rows[0][-3]
        if targeted_dead_blog is None:
            targeted_dead_blog = posts_rows[0][-1]

        logging.warning(f'{BColors.GREENOK}Got {len(posts_rows)} \
posts and/or reblogs for {targeted_dead_blog}{BColors.ENDC}')

        if pill2kill.is_set():
            break

        rid_cache = set()
        for row in posts_rows:
            if pill2kill.is_set():
                break
            if row[1] is not None and row[1] in rid_cache:
                continue
            if row[1] is None and row[0] is not None:
                # this is an original post! #TODO scrape them too later, but only for NON-DEAD blogs
                # TODO: if row[1] is none, scrape the blog present in row[-1] because it's the actual origin blogname!
                # that blog probably died or got wiped and we have some posts from it
                continue
            rid_cache.add(row[1])
            requester.name = row[-1]
            requester.health = '' # hacky partial reset

            if requester.name in blog_404_cache:
                logging.debug(f"{requester.name} is 404 in cache, skipping.")
                rid_cache.remove(row[1])
                continue

            logging.warning(f'{BColors.YELLOW}Fetching reblogged post for {row[-3]}:\
 {posts_rows.index(row) + 1}/{len(posts_rows)}{BColors.ENDC}')

            db_health = []
            try:
                cur = con.cursor()
                cur.execute(r"""select HEALTH from blogs where blog_name = '"""
                + requester.name + r"""';""")
                db_health = cur.fetchone()
            except BaseException as e:
                logging.debug(f"{BColors.FAIL}Exception while looking for health\
 for {requester.name}: {e}{BColors.ENDC}")
            finally:
                con.rollback()

            requester.health = db_health[0]

            if requester.health == 'DEAD':
                logging.debug(f"{requester.name} is DEAD in DB, writing to cache.")
                blog_404_cache.add(requester.name)
                rid_cache.remove(row[1])
                continue

            try:
                post_get_wrapper(requester, update, deep_scrape, post_id=row[0])
            except:
                rid_cache.remove(row[1])
                continue

            if requester.health == 'DEAD':
                logging.debug(f"{requester.name} is 404, writing to cache.")
                blog_404_cache.add(requester.name)
                rid_cache.remove(row[1])
                continue

            try:
                blogslist, notes_count = parse_post_json(update)
                logging.debug(f"PostID {row[0]} blog {row[-1]} yielded \
bloglist: {blogslist} notes_count {notes_count}")
            except BaseException as e:
                #traceback.print_exc()
                rid_cache.remove(row[1])
                logging.debug(f'{BColors.FAIL}Error getting notes: {e}{BColors.ENDC}')
                continue

            if pill2kill.is_set():
                break

            with db_update_lock:
                # Insert blog names into BLOGS table
                for blogname in blogslist:
                    try:
                        db_handler.insert_blogname_gathered(con, blogname, 'new')
                    except:
                        pass

                # Update the Post_ID if we were missing the reblogged blogname
                if row[-3] is None:
                    try:
                        origin_blogname = db_handler.update_or_insert_post(
                            con, update, notes_count)
                        logging.warning(f"{BColors.GREEN}Origin blogname for \
post_id {row[0]} -> remote_id {row[1]} was actually {origin_blogname}{BColors.ENDC}")
                    except BaseException as e:
                        logging.debug(f"Exception in update_or_insert_post: {e}")

                # Update all remote IDs with this count of notes
                try:
                    db_handler.update_remote_ids_with_notes_count(db, con,
                    row[1], requester.name, notes_count)
                except:
                    raise

        logging.warning(f"{BColors.GREENOK}{BColors.GREEN}Done scraping reblogs \
for {targeted_dead_blog}{BColors.ENDC}")

        if pill2kill.is_set(): # probably useless here
            break


def post_get_wrapper(requester, update, deep_scrape, post_id):
    """Handle exceptions"""

    update.__init__()
    attempts = 0
    while not update.valid and attempts < 3:
        attempts += 1
        try:
            requester.api_get_request(update, deep_scrape,
            post_id=post_id)
            if update.valid:
                return
            else:
                continue
        except BaseException as e:
            traceback.print_exc()
            logging.error(f"{BColors.RED}{requester.name} \
Exception during request: {e}{BColors.ENDC}")

    logging.error(f"{BColors.RED}{requester.name} Too many request \
attempts or server issue during request. Skipping for now. {BColors.ENDC}")
    raise BaseException("Too many request attempts or server issue during request")


def parse_post_json(update):
    """count the number of notes, returns blog_names listed"""
    if not update.posts_response:
        raise BaseException("Response list was empty!")
    try:
        noteslist = update.posts_response[0].get('notes')
    except BaseException as e:
        logging.debug(f"Exception trying to get update.post_response[0][notes]: {e}")
        raise

    blogslist = set()
    notes_count = 0

    if noteslist is None:
        logging.warning(f"There was no notes returned post {update.posts_response[0].get('id')}")
        return blogslist, notes_count

    for note in noteslist:
        name = note.get('blog_name')
        if name is not None:
            blogslist.add(name)
        notes_count += 1

    return blogslist, notes_count


def process(db, lock, db_update_lock, pill2kill):
    """Thread process"""

    deep_scrape = {}
    if instances.my_args.deep_scrape:
        deep_scrape['notes_info'] = True
        deep_scrape['reblog_info'] = True

    con = db.connect()
    blog = TumblrBlog()

    while not pill2kill.is_set():
        try:
            blog_generator(db, con, lock, blog, pill2kill)
        except BaseException as e:
            logging.debug(f"{BColors.FAIL}Exception occured in blog_generator:\
 {e}{BColors.ENDC}")
            pill2kill.set()
            traceback.print_exc()
            break

        if blog.name is None:
            logging.warning(f"{BColors.DARKGRAY}No blog name fetched! No \
more to process?{BColors.ENDC}")
            break

        # instances.sleep_here(0,1)
        update = UpdatePayload()
        # blog.database = db
        # blog.con = con

        if blog.crawl_status == 'new': # not yet updated
            try:
                blog_status_check(db, con, blog, update, deep_scrape)
            except BaseException as e:
                logging.debug(f'{BColors.FAIL}Exception while status_check {e}{BColors.ENDC}')
                continue

            if blog.health == 'DEAD':
                logging.warning(BColors.LIGHTRED + \
                "{0} Blog appears to be dead!"\
                .format(blog.name) + BColors.ENDC)
                continue

            # we already have the first batch of posts, insert them
            if blog.offset == 0: # DB had no previous offset, it's brand new
                if blog.posts_scraped is None:
                    blog.posts_scraped = 0
                insert_posts(db, con, db_update_lock, blog, update)


        elif blog.crawl_status == 'resume':
            try:
                blog_status_check(db, con, blog, update, deep_scrape)
            except BaseException as e:
                logging.debug(f'{BColors.FAIL}Exception while status_check {e}{BColors.ENDC}')
                continue

            if blog.offset > 0: # skip this first response, go straight to our previous offset
                update.posts_response = []


        elif blog.crawl_status == 'DONE':
            try:
                blog_status_check(db, con, blog, update, deep_scrape)
            except BaseException as e:
                logging.debug(f'{BColors.FAIL}Exception while status_check {e}{BColors.ENDC}')
                continue
            insert_posts(db, con, db_update_lock, blog, update)
        else:
            logging.debug("{0}{1} CRAWL_STATUS was neither resume nor new nor done: {2}{3}"\
            .format(BColors.FAIL, blog.name, blog.crawl_status, BColors.ENDC))
            raise BaseException("CRAWL_STATUS was neither resume nor new nor done") #FIXME

        # pbar = init_pbar(blog, position=threading.current_thread().name)
        update_offset_if_new_posts(blog)
        response_error = 0

        while not pill2kill.is_set() and not blog.eof and response_error < 10:
            if blog.posts_scraped >= blog.total_posts or blog.offset >= blog.total_posts:
                logging.debug(f"{blog.name} before loop: \
posts_scraped {blog.posts_scraped} or offset {blog.offset} >= total_posts \
{blog.total_posts}, breaking loop!")
                break

            if not update.posts_response:  #FIXME could be some other field attached to blog

                logging.warning(f"{BColors.LIGHTYELLOW}{blog.name} \
Getting at offset {blog.offset} / {blog.total_posts}{BColors.ENDC}")

                try:
                    api_get_request_wrapper(
                        db, con, blog, update, deep_scrape, blog.crawl_status)
                except BaseException as e:
                    logging.debug(f"{BColors.FAIL}{blog.name} \
Exception in api_get_request_wrapper from loop! {e}{BColors.ENDC}")
                    break

                try:
                    check_header_change(db, con, blog, update)
                except:
                    logging.debug(f"{BColors.FAIL}{blog.name}\
Error checking header change! Breaking.{BColors.ENDC}")
                    break

                if not update.posts_response:
                    if blog.posts_scraped >= blog.total_posts or blog.offset >= blog.total_posts:
                        blog.eof = True
                    elif blog.posts_scraped < blog.total_posts and blog.offset < blog.total_posts:
                        blog.offset += 1
                        response_error += 1
                        logging.debug(f"{BColors.BOLD}Inc offset by one because \
nothing in response but not end of total_posts!{BColors.ENDC}")
                        continue
                    logging.debug(f"update.posts_response is {update.posts_response}! break")
                    break
            else:
                logging.debug(f"{blog.name} inserting new posts")

                try:
                    insert_posts(db, con, db_update_lock, blog, update)
                except:
                    raise

                if blog.posts_scraped >= blog.total_posts or blog.offset >= blog.total_posts :
                    logging.info(f"{blog.name} else loop: total_posts <= posts_scraped or >= offset, breaking loop!")
                    break

        # We're done, no more found
        check_blog_end_of_posts(db, con, blog)
        db_handler.update_blog_info(db, con, blog, ignore_response=True)

        logging.warning(f"{BColors.GREENOK}{BColors.BOLD}{BColors.GREEN}\
{blog.name} Done scraping. Total {blog.posts_scraped}/{blog.total_posts}{BColors.ENDC}")

        if pill2kill.is_set():
            break


    logging.warning(BColors.LIGHTGRAY + "Terminating thread {0}"\
    .format(threading.current_thread()) + BColors.ENDC)

    if blog is None or blog.name is None:
        return
    else:
        thread_good_cleanup(db, con, blog)



# def init_pbar(blog, position):
#     position = int(position[-1]) - 3
#     pbar = tqdm(unit="post", total=int(blog.total_posts), position=position)
#     pbar.write("thread position {0}".format(position))
#     return pbar

# def update_pbar(pbar, blog):
#     yield pbar.update(blog.offset)

def insert_posts(db, con, db_update_lock, blog, update):
    with db_update_lock:
        processed_posts, errors, dupes = db_handler.insert_posts(db, con, blog, update)
    blog.db_dupes += dupes
    posts_added = processed_posts - errors # added - errors
    if posts_added < 0:
        posts_added = 0
    blog.posts_scraped += posts_added
    blog.offset += processed_posts

    if posts_added == 0: # too many DB errors
        logging.debug(f"{BColors.RED}{BColors.BOLD}{blog.name} \
{posts_added} posts_added due to many BD errors{BColors.ENDC}")

        if not instances.my_args.ignore_duplicates:
            logging.warning(f"{BColors.RED}{BColors.BOLD}{blog.name} setting crawling 2 \
because {posts_added} post added this round, too many errors?{BColors.ENDC}")
            blog.crawling = 2

    # We assume we are redoing previously done posts
    if blog.db_dupes >= 200 and not instances.my_args.ignore_duplicates: #FIXME arbitrarily
        logging.warning(f"{BColors.RED}{BColors.BOLD}{blog.name} marking EOF \
because {blog.db_dupes} dupes accumulated.{BColors.ENDC}")
        blog.eof = True

    logging.warning(f"{BColors.LIGHTYELLOW}{blog.name} current posts scraped \
{blog.posts_scraped} Offset is now: {blog.offset}{BColors.ENDC}")

    db_handler.update_blog_info(db, con, blog, ignore_response=True)

    # we may have 0 due to dupes causing errors to negate our processed_posts count
    if blog.posts_scraped == 0:
        blog.posts_scraped = db_handler.get_scraped_post_num(db, con, blog)
        logging.warning(f"{blog.name} Adjusting back total post from Database\
 due to counting errors: {blog.posts_scraped}")



def check_blog_end_of_posts(db, con, blog):
    """Check if we have indeed done everything right"""

    logging.debug("{0} check_blog_end_of_posts".format(blog.name))

    discrepancy_check(db, con, blog)

    if blog.offset >= blog.total_posts:
        # HACK get actual post count... but not pretty afterwards
        if blog.posts_scraped < blog.total_posts:
            blog.posts_scraped = db_handler.get_scraped_post_num(db, con, blog)
            if blog.posts_scraped < blog.total_posts:
                blog.crawl_status = 'resume'
                blog.offset = blog.posts_scraped
                return

        logging.info("Marking {0} as DONE".format(blog.name))
        blog.crawl_status = 'DONE'
        blog.offset = 0
    elif blog.posts_scraped >= blog.total_posts:
        logging.info("Marking {0} as DONE".format(blog.name))
        blog.crawl_status = 'DONE'
        blog.offset = 0
    else:
        logging.info("Marking {0} as resume".format(blog.name))
        blog.crawl_status = 'resume'

    if blog.eof:
        if blog.offset < blog.total_posts:
            # rare case where despite what the api tells us, total_posts is wrong (deleted posts?)
            blog.crawl_status = 'DONE'
            blog.offset = 0
            blog.crawling = 0

    if blog.temp_disabled:
        blog.crawling = 2
    else:
        blog.crawling = 0

    if blog.posts_scraped == 0:
        blog.posts_scraped = db_handler.get_scraped_post_num(db, con, blog)
    return



def api_get_request_wrapper(db, con, blog, update, deep_scrape, crawl_status, post_id=None):
    """Updates the update, valid or invalid"""

    # Retry getting /posts until either 404 or success
    update.__init__()
    attempts = 0
    offset_attempts = 0
    while not update.valid and attempts < 3:
        attempts += 1
        try:
            blog.api_get_request(update, deep_scrape, api_key=None, reqtype="posts",
            post_id=post_id)
            if update.valid:
                return
            else:
                continue

        except APIServerError as e:
            logging.warning(f"{BColors.LIGHTRED}{blog.name} Server on hold, \
skipping offset {blog.offset}.{BColors.ENDC}")

            attempts = 0 # HACK force infinite attempts
            blog.offset += 1
            offset_attempts += 1
            if offset_attempts > 50:
                # will only be reset next script start
                # HACK skipping one to avoid getting stuck on subsequent restarts
                blog.offset -= 49
                blog.temp_disabled = True 
                break
            continue

        except BaseException as e:
            traceback.print_exc()
            logging.error(f"{BColors.RED}{blog.name} Exception during request: {e}{BColors.ENDC}")

            if crawl_status != 'resume':
                thread_premature_cleanup(db, con, blog, crawl_status)
            break
    logging.error(f"{BColors.RED}{blog.name} Too many request attempts or server \
issue during request. Skipping for now. {BColors.ENDC}")
    raise BaseException("Too many request attempts or server issue during request")



def discrepancy_check(db, con, blog):
    """Gets actual posts_scraped from DB in case it differs from total/offset"""

    if blog.offset != blog.posts_scraped or blog.total_posts < blog.posts_scraped:

        logging.debug(f"{BColors.DARKGRAY}{blog.name} Error: \
blog.offset={blog.offset} blog.posts_scraped={blog.posts_scraped}. \
Getting actual posts_scraped from DB{BColors.ENDC}")

        # getting actual posts_scraped
        blog.posts_scraped = db_handler.get_scraped_post_num(db, con, blog)

        logging.debug(BColors.DARKGRAY + "{0} Got {1} posts_scraped from DB"
        .format(blog.name, blog.posts_scraped) + BColors.ENDC)



def blog_status_check(db, con, blog, update, deep_scrape):
    """Returns True on update validated, otherwise false"""

    if blog.crawl_status == 'new':
        isnew = True
    else:
        isnew = False

    discrepancy_check(db, con, blog)

    try:
        api_get_request_wrapper(db, con, blog, update, deep_scrape, blog.crawl_status)
    except BaseException as e:
        logging.debug(f'{BColors.FAIL}Exception while api_get_request_wrapper {e}{BColors.ENDC}')
        raise

    if update.valid:
        # update and retrieve remaining blog info
        blog.total_posts = update.total_posts

        if blog.crawl_status is None or blog.crawl_status == 'new':
            blog.crawl_status = "resume"
        elif  blog.crawl_status == 'DONE':
            blog.crawl_status = None

        db_response = db_handler.update_blog_info(db, con, blog)

        logging.debug(f"{BColors.BLUE}{blog.name} Got DB response: {db_response}{BColors.ENDC}")

        if not check_db_init_response(db_response, blog, isnew=isnew):
            logging.debug("{} check_db_init_response: False".format(blog.name))
            return False

    if not update.valid:
        logging.error(f"{BColors.RED}{blog.name} Too many invalid request \
attempts! Aborting for now.{BColors.ENDC}")
        if isnew:
            thread_premature_cleanup(db, con, blog, 'new')
        return False

    return True



def check_db_init_response(db_response, blog, isnew=False):
    """Parse items returned by DB on blog update"""

    if not db_response:
        return
    if not db_response.get('last_total_posts'):
        db_response['last_total_posts'] = 0
    if not db_response.get('last_offset'):
        db_response['last_offset'] = 0

    if not isnew:
        if db_response['last_offset'] > blog.total_posts:
            logging.debug(BColors.BOLD + \
            "{0} last offset was {1} and superior or equal to current total posts {2}.\
 Resetting to 0.".format(blog.name, db_response['last_offset'], blog.total_posts) + BColors.ENDC)

            db_response['last_offset'] = 0


        if db_response['last_total_posts'] < blog.total_posts:

            blog.new_posts = (blog.total_posts - db_response['last_total_posts'])

            logging.warning(BColors.BOLD + \
            "{0} has been updated. Old total_posts {1}, new total_posts {2}. \
Offset will be pushed by {3}"\
            .format(blog.name, db_response.get('last_total_posts'), \
            blog.total_posts, blog.new_posts) + BColors.ENDC)


        elif db_response.get('last_total_posts', 0) > blog.total_posts:

            logging.error(f"{BColors.FAIL}{BColors.BOLD}{blog.name} \
WARNING: number of posts has decreased from {db_response.get('last_total_posts')} to {blog.total_posts}!\n\
Blog was recently updated {db_response.get('last_updated')}, previously checked on {db_response.get('last_checked')}\n\
Did the author remove posts {int(db_response.get('last_total_posts', 0)) - blog.total_posts}!?{BColors.ENDC}")
            return False


        if db_response.get('last_health').find("UP") and \
            db_response.get('last_health') != blog.health: # health changed!! Died suddenly?

            logging.warning(BColors.FAIL + BColors.BOLD + \
            "{0} WARNING: changed health status from {1} to: {2}"\
            .format(blog.name, blog.health, db_response.get('last_health')) + BColors.ENDC)
            return False

    # initializing
    # logging.warning("{0} initializing offset to what it was in DB".format(blog.name))
    if isnew:
        blog.offset = db_response.get('last_offset', 0)
        blog.posts_scraped = db_response.get('last_scraped_posts', 0)

    blog.crawling = 1 # set by DB by procedure on its side
    blog.db_response = db_response
    # logging.warning("{0} check_db_init_response returning true".format(blog.name))
    return True


def check_header_change(database, con, blog, update):
    """Check if the header changed, update info in DB if needed"""

    if update.total_posts > blog.total_posts:
        # FIXME we have new posts, increment current offset
        logging.warning("{0}{1.name} just got new posts in header! Updating DB.{2}"
        .format(BColors.BOLD, blog, BColors.ENDC))
        blog.db_response = db_handler.update_blog_info(database, con, blog)

    blog.total_posts = update.total_posts
    blog.last_updated = update.updated


def update_offset_if_new_posts(blog):
    """Increments offset if new posts arrived while scraping, according to header"""

    # to avoid re-inserting previously inserted posts
    if blog.new_posts > 0:
        blog.offset += blog.new_posts
        logging.info(f"{BColors.RED}{blog.name} Offset incremented because of \
new posts by +{blog.new_posts}: {blog.offset}{BColors.ENDC}")
        blog.new_posts = 0


def blog_generator(db, con, lock, blog, pill2kill):
    """Queries DB for a blog that is either new or needs update.
    Returns a TumblrBlog() object instance with no proxy attached to it."""

    blog.__init__(pill2kill=pill2kill, lock=lock)
    with lock:
        blog.name, blog.offset, blog.health, blog.crawl_status, blog.total_posts, \
blog.posts_scraped, blog.last_checked, blog.last_updated = db_handler.fetch_random_blog(
            db, con, status_req=None)

    if not blog.name:
        logging.debug(f"{BColors.RED}No blog fetched in blog_generator(){BColors.ENDC}")
        return blog

    if blog.offset is None:
        blog.offset = 0

    if blog.posts_scraped is None:
        blog.posts_scraped = 0

    if blog.total_posts is None:
        blog.total_posts = 0

    # attach a proxy
    blog.attach_proxy()
    # init requests.session with headers
    blog.init_session()
    try:
        blog.attach_random_api_key()
    except:
        raise
    logging.info(BColors.CYAN + "{0} Got blog from DB."\
    .format(blog.name) + BColors.ENDC)

    debug_slots = ''
    for slot in blog.__slots__:
        debug_slots = f'{debug_slots} {slot}= {str(getattr(blog, slot))}'
    logging.debug(f"{BColors.CYAN}blog fields: {debug_slots}{BColors.ENDC}")



class TumblrBlog:
    """blog object, holding retrieved values to pass along"""

    __slots__ = ('name', 'total_posts', 'posts_scraped', 'offset', 'health',
    'crawl_status', 'crawling', 'last_checked', 'last_updated', 'proxy_object',
    'api_key_object_ref', 'requests_session', 'current_json', 'update',
    'new_posts', 'temp_disabled', 'eof', 'pill2kill', 'lock', 'db_response', 'db_dupes')

    def __init__(self, *args, **kwargs):
        self.name = None
        self.total_posts = 0
        self.posts_scraped = 0
        self.offset = 0
        self.health = None
        self.crawl_status = None
        self.crawling = 0
        self.last_checked = None
        self.last_updated = None
        self.proxy_object = None
        self.api_key_object_ref = None
        self.requests_session = None
        self.current_json = None
        self.update = None
        self.new_posts = 0
        self.temp_disabled = False
        self.eof = False
        self.pill2kill = kwargs.get('pill2kill')
        self.lock = kwargs.get('lock')
        self.db_response = None
        self.db_dupes = 0

    def init_session(self):
        if not self.requests_session: # first time
            requests_session = requests.Session()
            requests_session.headers.update(
            {'User-Agent': self.proxy_object.get('user_agent')})
            requests_session.proxies.update(
            {'http': self.proxy_object.get('ip_address'),
            'https': self.proxy_object.get('ip_address')})
            self.requests_session = requests_session
        else:
            self.requests_session.headers.update(
            {'User-Agent': self.proxy_object.get('user_agent')})
            self.requests_session.proxies.update(
            {'http': self.proxy_object.get('ip_address'),
            'https': self.proxy_object.get('ip_address')})


    def attach_proxy(self, proxy_object=None):
        """ attach proxy object, refresh session on update too"""

        if not proxy_object: #common case
            while not proxy_object:
                proxy_object = next(instances.proxy_scanner.proxy_ua_dict.get('proxies'))
                if not proxy_object:
                    with self.lock:
                        proxy_object = instances.proxy_scanner.get_new_proxy(self.pill2kill)

        if not self.proxy_object: #first time
            self.proxy_object = proxy_object
        else: #we're updating
            self.proxy_object = proxy_object
            self.init_session() # refresh


    def attach_random_api_key(self):
        """ attach api key fetched from global list to proxy object already attached
        if no key is available anymore, sleep until one becomes available again"""

        while True:
            try:
                self.api_key_object_ref = api_keys.get_random_api_key(instances.api_keys)
                logging.debug(f"APIKEY attached to {self.name}: \
{self.api_key_object_ref.api_key}")
                # self.proxy_object.api_key = temp_key.api_key
                # self.proxy_object.secret_key =  temp_key.secret_key
                # attach string to local proxy dict, in case we need to keep the proxy for later use
                self.proxy_object.update({'api_key': self.api_key_object_ref.api_key})
                return
            except api_keys.APIKeyDepleted as e:
                # all keys are currently disabled or blacklisted
                timetosleep = ((e.next_date_avail - time.time()) + random.choice(range(60, 100)))
                logging.warning(f"{BColors.MAGENTA}Sleeping for {timetosleep} \
seconds until {time.ctime(e.next_date_avail)}{BColors.ENDC}")

                while not self.pill2kill.is_set():
                    time.sleep(5)
                    timetosleep -= 5
                    if timetosleep <= 0:
                        break
                    else:
                        continue
                else:
                    break

                logging.warning(f'{BColors.LIGHTGRAY}Waking up from slumber{BColors.ENDC}')
                continue

            except (AttributeError, BaseException) as e:
                logging.debug(f'{BColors.FAIL}Exception in attach_random_api_key:{e}{BColors.ENDC}')
                raise
        # We asked for termination, and there is still no API key set! # FIXME custom exception
        raise BaseException("Asked for early termination")


    def blacklist_api_key(self, immediate=False, old_api_key=None):
        """Mark key as to be blacklisted after 3 hits"""
        if not old_api_key:
            old_api_key = self.api_key_object_ref

        if old_api_key.blacklist_hit >= 3 or immediate:
            logging.critical("{0}Blacklisting api key {1}!{2}"
            .format(BColors.MAGENTA, old_api_key.api_key, BColors.ENDC))

            api_keys.disable_api_key(old_api_key, blacklist=True)
        else:
            logging.info(f"{BColors.MAGENTA}Warning for API key \
{old_api_key.api_key}{BColors.ENDC}")
            old_api_key.blacklist_hit += 1

        try:
            self.attach_random_api_key()
        except:
            raise

    def get_new_proxy(self, old_proxy_object=None):
        """ Pops old proxy gone bad from cycle, get a new one """
        if not old_proxy_object:
            old_proxy_object = self.proxy_object

        with self.lock: # get_new_proxy() requires a lock!
            self.proxy_object = instances.proxy_scanner.get_new_proxy(self.pill2kill,
                old_proxy_object, remove='remove')

        try:
            self.attach_random_api_key()
        except:
            raise

        self.init_session() # refresh request session (proxy ip, user agent)

        logging.info(BColors.BLUEOK + "{0} Changed proxy to {1}"\
        .format(self.name, self.proxy_object.get('ip_address')) + BColors.ENDC)



    def api_get_request(self, updateobj, deep_scrape, api_key=None, reqtype="posts", post_id=None):
        """Returns requests.response object, reqype=[posts|info]"""
        if not api_key:
            api_key = self.api_key_object_ref

        if api_key.is_disabled():
            logging.debug(f"{BColors.MAGENTA}Before requests, \
API key {api_key.api_key} is disabled, trying to get a new one{BColors.ENDC}")
            try:
                self.attach_random_api_key()
                api_key = self.api_key_object_ref
            except:
                raise

        api_key.use()
        used_key = 0

        params = {}
        if self.offset is not None and self.offset != 0:
            params['offset'] = self.offset
            if deep_scrape:
                params.update(deep_scrape)
        if post_id is not None: # we ask for just a post_id
            params['id'] = post_id
            if deep_scrape:
                params.update(deep_scrape)
            params['reblog_info'] = True

        instances.sleep_here(0, THREADS)
        attempt = 0
        response = requests.Response()
        response_json = {'meta': {'status': 500, 'msg': 'Server Error'},
        'response': [], 'errors': [{"title": "Malformed JSON or HTML was returned."}]}

        if not self.requests_session:
            self.init_session()
            logging.debug("---\n{0}Initializing new requests session: {1} {2}\n----"\
            .format(self.name, self.requests_session.proxies,
            self.requests_session.headers))

        url = f'https://api.tumblr.com/v2/blog/{self.name}/{reqtype}?api_key={api_key.api_key}'

        if params:
            for param in params.keys():
                url = f'{url}&{param}={params[param]}'

        while attempt < 10:
            attempt += 1
            used_key += 1
            try:
                logging.info(BColors.GREEN + BColors.BOLD +
                "{0} GET ip: {1} url: {2}".format(self.name,
                self.proxy_object.get('ip_address'), url) + BColors.ENDC)

                if reqtype == "posts": # no need OAuth
                    response = self.requests_session.get(url=url, timeout=20)
                else: # need OAuth for "likes" or "followers"
                    response = self.requests_session.get(url=url, oauth=api_key.oauth, timeout=20)

                try:
                    response_json = self.parse_json(response)
                except:
                    raise

            except (requests.exceptions.ProxyError, requests.exceptions.Timeout) as e:
                logging.info(BColors.FAIL + "{0} Proxy {1} error: {2}"\
                .format(self.name, self.proxy_object.get('ip_address'), e.__repr__()) + BColors.ENDC)
                used_key -= 1
                try:
                    self.get_new_proxy()
                except:
                    continue
                continue

            except (ConnectionError, requests.exceptions.RequestException) as e:
                logging.info(BColors.FAIL + "{0} Connection error Proxy {1}: {2}"
                .format(self.name, self.proxy_object.get('ip_address'), e.__repr__()) + BColors.ENDC)
                used_key -= 1
                try:
                    self.get_new_proxy()
                except:
                    continue
                continue

            except (json.decoder.JSONDecodeError) as e:
                logging.info(f"{BColors.FAIL}Fatal error decoding json, maybe we should be \
removing proxy {self.proxy_object.get('ip_address')}? (continue){BColors.ENDC}")

                if response.text.find('Service is temporarily unavailable') != -1:
                    logging.warning(f"{BColors.BOLD}Api error from: {url}{BColors.ENDC}")
                    raise APIServerError()
                #else: self.get_new_proxy()
                continue

            except:
                logging.debug(f"{BColors.FAIL}Uncaught exception in request! (continue){BColors.ENDC}")
                continue

            break

        if used_key <= 0:
            api_key.refund()
        elif used_key > 1:
            api_key.use(use_count=used_key - 1)

        try:
            self.check_response_validate_update(response_json, updateobj, self.lock)
        except:
            raise



    def parse_json(self, response):
        """Parse requests.Response() to get json"""
        try:
            response_json = response.json()
        except (ValueError, json.decoder.JSONDecodeError):
            logging.warning(
                f"{BColors.YELLOW}{self.name} Error trying to parse response into \
json. Exerpt: {response.text[:5000]}{BColors.ENDC}")

            if response.text.find('Service is temporarily unavailable. \
Our engineers are working quickly to resolve the issue') != -1:
                raise

            if response.text.find('"response":{') != -1:
                try:
                    response_json = response.text.split('''"response":{''')[1]
                    # for some reason, sometimes it fails there?
                    response_json = r'{"meta": \
{"status": 200,"msg": "OK","x_tumblr_content_rating": "adult"},' + response_json[:-1]
                    logging.debug(f"{BColors.YELLOW}split: {response_json[:5000]}{BColors.ENDC}")
                    try:
                        response_json = response.json()
                    except:
                        logging.debug(f"{BColors.FAIL}Really couldn't get a good\
 json!\nProxy: {self.proxy_object.get('ip_address')}\n{response_json[:5000]}{BColors.ENDC}")
                        raise
                except:
                    response_json = {'meta': {'status': 500, 'msg': 'Server Error'},
                    'response': [], 'errors': [{"title": "Malformed JSON or HTML was returned."}]}
        except:
            logging.exception(f"{BColors.YELLOW}{self.name}Uncaught fatal error \
trying to parse json from response: {response.text}{BColors.ENDC}")
            raise
        return response_json


    def check_response_validate_update(self, response_json, update, lock):
        """ Reads the response object, updates the blog attributes accordingly.
        Last checks before updating BLOG table with info
        if unauthorized in response, change API key here, etc."""

        logging.debug(f"{BColors.LIGHTCYAN}{self.name} Before parsing reponse \
check_response_validate_update response_json status={response_json.get('meta').get('status')} \
response_json msg {response_json.get('meta').get('msg')}{BColors.ENDC}")

        logging.debug(f"{BColors.LIGHTCYAN}{self.name} JSON is: \
{str(response_json)[:1000]}{BColors.ENDC}")

        update.meta_status = response_json.get('meta').get('status')
        update.meta_msg = response_json.get('meta').get('msg')


        if not response_json.get('response') or not (200 <= update.meta_status <= 399):
            logging.debug(f"{BColors.BOLD}{self.name} Got errors in Json! response:\
 {response_json.get('response')} meta_status: {update.meta_status}{BColors.ENDC}")

        if update.meta_status == 403:
            self.health = "DEAD"
            self.crawl_status = "DEAD"
            update.valid = True
            return

        resp_json = response_json.get('response')
        if resp_json is not None and resp_json != [] and isinstance(dict(), type(resp_json)):
            update.blogname = resp_json.get('blog', {}).get('name')
            update.total_posts = resp_json.get('blog', {}).get('total_posts')
            update.updated = resp_json.get('blog', {}).get('updated')
            update.posts_response = resp_json.get('posts', []) #list of dicts
        elif isinstance(str(), type(resp_json)) and resp_json.find("Service is temporarily unavailable") != -1:
            raise APIServerError()


        logging.debug(f"{BColors.LIGHTCYAN}{self.name} After parsing reponse, \
check_response_validate_update update.meta_msg={update.meta_msg} \
update.meta_status {update.meta_status}{BColors.ENDC}")

        if response_json.get('errors') is not None:
            if update.meta_status == 404 or update.meta_msg.find('Not Found') != -1:
                logging.warning(f"{BColors.FAIL}{self.name} update has \
404 error status {update.meta_status} {update.meta_msg}! \
Setting to DEAD!{BColors.ENDC}")
                self.health = "DEAD"
                self.crawl_status = "DEAD"
                update.valid = True
                return

            if response_json.get('errors', [{}])[0].get('title', str()).find("error") != -1 and\
                response_json.get('errors', [{}])[0].get('title', str()).find("Unauthorized") != -1: #FIXME
                logging.critical(f"{BColors.FAIL}{self.name} is unauthorized! \
Rolling for a new API key.\n{response_json}{BColors.ENDC}")
                # FIXME: that's assuming only the API key is responsible for unauthorized, might be the IP!
                # Examples: {"meta":{"status":401,"msg":"Unauthorized"},"response":[],
                with lock:
                    self.blacklist_api_key()
                update.valid = False
                return

            if update.meta_status == 429 or update.meta_msg.find("Limit Exceeded") != -1:
                # 'meta_status': 429, 'meta_msg': Limit Exceeded'
                logging.critical(f"{BColors.RED}{BColors.BLINKING}{self.name} Limit Exceeded 429 error{BColors.ENDC}")

                # Renew API key
                logging.critical(f"Renewing API key {self.api_key_object_ref.api_key}")
                with lock:
                    api_keys.disable_api_key(self.api_key_object_ref)
                try:
                    self.attach_random_api_key()
                except:
                    raise

                update.valid = False
                return #FIXME returning with a now valid api_key only to reroll the same -> twice


            logging.error(BColors.FAIL +
            "{0} uncaught fatal error in response: {1}"
            .format(self.name, repr(update.__dict__)[:1000]) + BColors.ENDC)
            update.valid = False
            return

        update.valid = True
        self.health = "UP"
        self.crawling = 1

        if update.total_posts < 20:   #FIXME: arbitrary value
            logging.info("{0} considered WIPED!".format(self.name))
            self.health = "WIPED"

        logging.debug(BColors.BLUEOK + \
        "{0} No error in check_response_validate_update()"\
        .format(self.name) + BColors.ENDC)


class APIServerError(Exception):
    """We assume we have checked that all API keys are now disabled"""
    def __init__(self, message=None):
        if not message:
            message = "API error from server"
        super().__init__(message)


class Requester(TumblrBlog):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.attach_proxy()
        # init requests.session with headers
        self.init_session()
        self.attach_random_api_key()


class UpdatePayload(requests.Response):
    """ Container dictionary holding values from json to pass along """
    def __init__(self):
        self.errors_title = str()
        self.valid = False
        self.meta_status = None
        self.meta_msg = None
        self.total_posts = None
        self.updated = None
        self.posts_response = []


class SignalHandler:
    """The object that will handle signals and stop the worker threads.
    https://christopherdavis.me/blog/threading-basics.html"""

    def __init__(self, stopper, workers):
        self.stopper = stopper
        self.workers = workers

    def __call__(self, signum, frame):
        self.stopper.set()
        # for worker in self.workers:
        #     worker.join()
        # print("Exiting")
        # sys.exit(0)


def final_cleanup(proxy_scanner):
    """Main cleanup before exit, writing JSONs to disk"""
    logging.debug(f"{BColors.LIGHTGRAY}final_cleanup, writing keys and \
proxies to json{BColors.ENDC}")

    proxy_scanner.write_proxies_to_json_on_disk()
    api_keys.write_api_keys_to_json()


def thread_good_cleanup(db, con, blog):
    """Cleanup when thread is terminated"""

    logging.debug(BColors.LIGHTGRAY + \
    "{0} thread_good_cleanup -> update_blog_info"\
    .format(blog.name) + BColors.ENDC )

    blog.crawling = 0
    # db_handler.update_crawling(con, blog.name)
    db_handler.update_blog_info(db, con, blog, ignore_response=True)


def thread_premature_cleanup(db, con, blog, reset_type):
    """Blog has not been updated in any way, just reset STATUS to NEW if was INIT"""

    logging.debug(f"{BColors.LIGHTGRAY}{blog.name} thread_premature_cleanup, \
reset_to_brand_new '{reset_type}'{BColors.ENDC}")

    # only resets CRAWL_STATUS to 'new', not CRAWLING which stays 1 to avoid repicking it straight away
    db_handler.reset_to_brand_new(db, con, blog, reset_type=reset_type)


def setup_config(args):

    # logging.basicConfig(format='{levelname}:    \t{message}', style='{',
    #                     level=getattr(logging, args.log_level.upper()))
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)
    # rootLogger.propagate = False

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(getattr(logging, args.log_level.upper()))
    sh.setFormatter(logging.Formatter('{asctime} {levelname:<9}:{threadName:>5}\t{message}', '%y/%m/%d %H:%M:%S', '{'))
    rootLogger.addHandler(sh)

    instances.config = parse_config(args.config_path, args.data_path)

    fh = logging.handlers.RotatingFileHandler(filename=instances.config.get('tumblrmapper', 'log_path'),
                            mode='a', maxBytes=10000000, backupCount=20)
    fh.setLevel(getattr(logging, instances.config.get('tumblrmapper', 'log_level')))
    fh.setFormatter(logging.Formatter(
                    '{asctime} {levelname:<9}:{threadName:>5}\t{message}',
                    '%y/%m/%d %H:%M:%S', '{'))
    rootLogger.addHandler(fh)

    logging.debug("Debugging Enabled.")
    return rootLogger


def init_global_api_keys():
    """Instantiate list of api keys in global and starts the thread to look over them"""
    # === API KEY ===
    # list of APIKey objects
    instances.api_keys = api_keys.get_api_key_object_list(SCRIPTDIR + os.sep
    + instances.config.get('tumblrmapper', 'api_keys'))

    # Initialize use counter for API keys
    api_keys.threaded_buckets()


def init_global_proxies(THREADS, pill2kill):
    # === PROXIES ===
    # Get proxies from free proxies site
    instances.proxy_scanner = ProxyScanner(proxies_path=instances.config.get('tumblrmapper', 'proxies'))

    if len(instances.proxy_scanner.proxy_ua_dict.get('proxies')) <= THREADS:
        instances.proxy_scanner.get_proxies_from_internet(minimum=THREADS, pill2kill=pill2kill)

    logging.debug(f"Will use this proxy listcycle: {instances.proxy_scanner.proxy_ua_dict.get('proxies')}")

    # fresh_proxy_dict = {'proxies': [{'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36(KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '47.206.51.67:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36', 'disabled': False}, {'ip_address': '92.53.73.138:8118', 'user_agent': 'Mozilla/5.0 (Windows NT6.1; WOW64; rv:21.0) Gecko/20130331 Firefox/21.0', 'disabled': False}, {'ip_address': '45.77.247.164:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '80.211.4.187:8080', 'user_agent': 'Mozilla/5.0 (Microsoft Windows NT 6.2.9200.0); rv:22.0) Gecko/20130405 Firefox/22.0', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '66.82.123.234:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64;x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh;Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '52.164.249.198:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '47.206.51.67:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36', 'disabled':False}, {'ip_address': '92.53.73.138:8118', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130331 Firefox/21.0', 'disabled': False}, {'ip_address': '45.77.247.164:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '80.211.4.187:8080', 'user_agent': 'Mozilla/5.0 (Microsoft Windows NT 6.2.9200.0); rv:22.0) Gecko/20130405 Firefox/22.0', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '191.34.157.243:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36', 'disabled': False}, {'ip_address': '61.91.251.235:8080', 'user_agent': 'Opera/9.80 (Windows NT 5.1; U; zh-tw) Presto/2.8.131 Version/11.10', 'disabled': False}, {'ip_address': '41.190.33.162:8080', 'user_agent': 'Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36', 'disabled': False}, {'ip_address': '80.48.119.28:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '213.99.103.187:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36', 'disabled': False}, {'ip_address': '141.105.121.181:80', 'user_agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)', 'disabled': False}]}


def main(args):
    global THREADS
    THREADS = instances.config.getint('tumblrmapper', 'threads')

    if args.create_blank_db or args.update_blogs or args.update_archives or args.create_archive_list:

        if args.create_archive_list:
            archive_lists.main(output_pickle=True)

        if args.create_blank_db:
            database = db_handler.Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath')
                            + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                            username=instances.config.get('tumblrmapper', 'username'),
                            password=instances.config.get('tumblrmapper', 'password'))
            try:
                db_handler.create_blank_database(database)
            except Exception as e:
                logging.error(BColors.FAIL + "Database creation failed:{0}"
                .format(e) + BColors.ENDC)

        if args.update_blogs:
            blogs_toscrape = instances.config.get('tumblrmapper', 'blogs_to_scrape')
            database = db_handler.Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath')
                            + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                            username=instances.config.get('tumblrmapper', 'username'),
                            password=instances.config.get('tumblrmapper', 'password'))
            db_handler.populate_db_with_blogs(database, blogs_toscrape)
            logging.warning(BColors.BLUEOK + BColors.GREEN + "Done inserting blogs"
             + BColors.ENDC)

        if args.update_archives:
            archives_toload = instances.config.get('tumblrmapper', 'archives')
            database = db_handler.Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath')
                            + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                            username=instances.config.get('tumblrmapper', 'username'),
                            password=instances.config.get('tumblrmapper', 'password'))
            if "pickle" in archives_toload:
                db_handler.update_db_with_archives(database,
                archives_toload, use_pickle=True)
            else:
                db_handler.update_db_with_archives(database,
                archives_toload, use_pickle=False)
            logging.warning(BColors.BLUEOK + BColors.GREEN +
            "Done inserting archives" + BColors.ENDC)

        return

    instances.my_args = args

    # === DATABASE ===
    db = db_handler.Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath')
                            + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                            username=instances.config.get('tumblrmapper', 'username'),
                            password=instances.config.get('tumblrmapper', 'password'))


    os.nice(instances.config.getint('tumblrmapper', 'nice_level'))

    worker_threads = []

    pill2kill = threading.Event()
    lock = threading.Lock()
    db_update_lock = threading.Lock()

    # Handle pressing ctrl+c on Linux?
    signal_handler = SignalHandler(pill2kill, worker_threads)
    signal.signal(signal.SIGINT, signal_handler)

    # Thread handling keyboard input to interrupt
    input_t = threading.Thread(target=input_thread, args=(pill2kill, worker_threads))
    input_t.daemon = True
    worker_threads.append(input_t)

    # Modules / plugins:
    if args.compute_hashes or args.match_hashes:
        init_global_api_keys()
        input_t.start()
        init_global_proxies(THREADS=1, pill2kill=pill2kill)
        kwargs_module = dict(pill2kill=pill2kill, lock=lock, db=db)
        hashes.main(args, kwargs_module) # FIXME: pass file to write from config as argument too
        return

    init_global_api_keys()
    input_t.start()
    init_global_proxies(THREADS, pill2kill)

    # Reset crawling status for all blogs in DB
    con = db.connect()
    db_handler.update_crawling(con)
    db.close_connection(con)

    # Common thread arguments
    thread_args = (db, lock, db_update_lock, pill2kill)

    if instances.my_args.scrape_notes:
        # forcing deep scrape to get notes
        instances.my_args.deep_scrape = True
        tgt = process_notes
        thread_args = thread_args + (args.scrape_notes,)
    else:
        tgt = process

    for _ in range(0, THREADS):
        t = threading.Thread(target=tgt, args=thread_args)
        worker_threads.append(t)
        # t.daemon = True
        t.start()

    # start incrementing API keys's buckets of tokens
    logging.warning(BColors.BLUEOK + "Starting scraping" + BColors.ENDC)

    with futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(worker_threads)

    # def worker_get_from_queue(q):
    #     while True:
    #         item = q.get()
    #         if item is end_of_queue:
    #             q.task_done()
    #             break
    #         do_work(item)
    #         q.task_done()

    for t in worker_threads:
        t.join()

    logging.debug(BColors.LIGHTGRAY + "Done with all threads" + BColors.ENDC)

    # ============= FUTURES TEST =======================
    # with futures.ThreadPoolExecutor(THREADS) as executor:
    #     info_jobs = [executor.submit(blog.api_get_blog_json_health) for blog in blog_object_list]
    #     futures.wait(info_jobs, timeout=10, return_when=futures.ALL_COMPLETED)
    #     results = [job.result() for job in info_jobs]
    #     process_job = [executor.submit(update_blog_health_db, blog, result) for result in results]
        # process_job.add_done_callback()
    # ============= FUTURES TEST =======================

    # # When prerequisite is completed
    # with futures.ThreadPoolExecutor(THREADS) as executor:
    #     jobs = [executor.submit(get_posts, blog) for blog in blogs] #"blog" is an iterable of single arguments
    #     if keyboard_interrupt:
    #         for job in jobs:
    #             jobs.cancel()
    #     for comp_job in futures.as_completed(jobs):
    #         response = comp_job.result()
    #         executor.submit(insert_into_db, response)

    # exit
    db.close_connection()
    final_cleanup(instances.proxy_scanner)
    return


if __name__ == "__main__":
    # window = curses.initscr()
    # window.nodelay(True)
    # curses.echo()
    # curses.cbreak()

    # parse command-line arguments
    args = parse_args()
    setup_config(args)

    main(args)
