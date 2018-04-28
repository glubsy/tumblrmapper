#!/bin/env python3
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
import signal
import traceback
from concurrent import futures
from itertools import cycle
import requests
import api_keys
import db_handler
import instances
import proxies
from constants import BColors
import update_archive_lists
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
# import curses
# import ratelimit

SCRIPTDIR = os.path.dirname(__file__)
THREADS = 10
asked_termination = False

def parse_args():
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description='tumblrmapper tumblr url mapper.')
    parser.add_argument('-c', '--config_path', action="store",
                        help="Path to config directory.")
    parser.add_argument('-d', '--data_path', action="store", default=None,
                        help="Set default path to data directory, where logs and DB are stored")
    parser.add_argument('-l', '--log_level', action="store", default="CRITICAL",
                        help="Set log level: DEBUG, INFO, WARNING, ERROR, CRITICAL (default)")

    # actiongrp = parser.add_mutually_exclusive_group()
    parser.add_argument('-u', '--update_archive_list', action="store_true",
                    help="Recreate archive file listing.")

    parser.add_argument('-n', '--create_blank_db', action="store_true",
                        help="Create a blank DB in data_dir and populate it")
    parser.add_argument('-s', '--update_archives', action="store_true",
                    help="Populate DB with archives")
    parser.add_argument('-b', '--update_blogs', action="store_true",
                    help="Populate DB with blogs")

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
    logging.debug("Trying to read config file: %s", config_path)
    result = config.read(config_path)
    if not result:
        logging.warning("Unable to read config file: %s", config_path)

    config.set('tumblrmapper', 'blogs_to_scrape', \
    SCRIPTDIR + os.sep + config.get('tumblrmapper', 'blogs_to_scrape'))

    config.set('tumblrmapper', 'archives', \
    SCRIPTDIR + os.sep + config.get('tumblrmapper', 'archives'))

    config.set('tumblrmapper', 'db_filepath', \
    os.path.expanduser(config.get('tumblrmapper', 'db_filepath')))

    logging.debug("Merged config: %s",
                sorted(dict(config.items('tumblrmapper')).items()))

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
            event.set()

    # while True:
    #     if input():
    #         print(qlist)
    #         qlist.append(None)


def process(db, lock, db_update_lock, pill2kill):

    con = db.connect()
    blog = None

    while not pill2kill.is_set():

        with lock:
            try:
                blog = blog_generator(db, con)
            except:
                pill2kill.set()

        if blog.name is None:
            logging.warning(BColors.DARKGRAY + \
            "No blog name fetched! No more to process?" + BColors.ENDC)
            break

        update = UpdatePayload()

        # blog.database = db
        # blog.con = con

        if blog.crawl_status == 'new': # not yet updated

            if not blog_status_check(db, con, lock, blog, update):
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
            if not blog_status_check(db, con, lock, blog, update):
                continue
            if blog.offset > 0: # skip this first response, go straight to our previous offset
                update.posts_response = []


        elif blog.crawl_status == 'DONE':
            if not blog_status_check(db, con, lock, blog, update):
                continue
            insert_posts(db, con, db_update_lock, blog, update)

        else:
            raise Exception(BColors.FAIL + \
            "{0} CRAWL_STATUS was neither resume nor new nor done: {1}"\
            .format(blog.name, blog.crawl_status) + BColors.ENDC)


        update_offset_if_new_posts(blog)

        while not pill2kill.is_set():
            if blog.posts_scraped >= blog.total_posts or blog.offset >= blog.total_posts:
                logging.debug(
                "{0} before loop: posts_scraped {1} or offset {2} >= total_posts {3}, breaking loop!"
                .format(blog.name, blog.posts_scraped, blog.offset, blog.total_posts))
                break

            if not update.posts_response:  # could be some other field attached to blog
                logging.warning(BColors.LIGHTYELLOW \
                + "{0} Getting at offset {1} / {2}".format(blog.name, blog.offset, blog.total_posts) + BColors.ENDC)

                try:
                    if not api_get_request_wrapper(db, con, lock, blog, update, blog.crawl_status, offset=blog.offset):
                        logging.debug(BColors.FAIL + "{0} request_wrapper returns fase, break!".format(blog.name) + BColors.ENDC)
                        break
                except Exception as e:
                    logging.info(BColors.FAIL + "Exception in api_get_request_wrapper from loop! {0}"
                    .format(e) + BColors.ENDC)
                    break

                check_header_change(db, con, blog, update)

                if not update.posts_response: # nothing more
                    logging.debug("update.posts_response is {0}! break".format(repr(update.posts_response)))
                    break
            else:
                logging.debug("{0} inserting new posts".format(blog.name))

                insert_posts(db, con, db_update_lock, blog, update)
                # update_pbar(blog, position=threading.current_thread())

                if blog.posts_scraped >= blog.total_posts or blog.offset >= blog.total_posts :
                    logging.debug("{0} else loop: total_posts >= posts_scraped or >= offset, breaking loop!"
                    .format(blog.name))
                    break

        # We're done, no more found
        check_blog_end_of_posts(db, con, blog)
        db_handler.update_blog_info(db, con, blog, ignore_response=True)

        logging.warning(BColors.GREENOK + BColors.BOLD + BColors.GREEN + 
        "{0} Done scraping. Total {1}/{2}"
        .format(blog.name, blog.posts_scraped, blog.total_posts) + BColors.ENDC)

        if pill2kill.is_set():
            break


    logging.debug(BColors.LIGHTGRAY + "Terminating thread {0}"\
    .format(threading.current_thread()) + BColors.ENDC)

    if blog is None or blog.name is None:
        return
    else:
        thread_good_cleanup(db, con, blog)



def init_pbar(position)
    position = position[-1] - 3
    pbar = tqdm(unit="post", total=int(blog.total_posts), position=postition)
    pbar.write("thread position {0}".format(position))

def update_pbar(pbar, blog):
    pbar.update(blog.offset)


def insert_posts(db, con, db_update_lock, blog, update):
    with db_update_lock:
        processed_posts, errors = db_handler.insert_posts(db, con, blog, update)
        blog.posts_scraped += processed_posts - errors # added - errors
        blog.offset += processed_posts

        logging.debug("{0} Posts just scraped {1} Offset is now: {2}"\
        .format(blog.name, blog.posts_scraped, blog.offset))

    # we may have 0 due to dupes causing errors to negate our processed_posts count
    if blog.posts_scraped == 0:
        blog.posts_scraped = db_handler.get_total_post(db, con, blog)
        logging.info("{0} Adjusting back total post from Database due to errors: {1}"
        .format(blog.name, blog.posts_scraped))



def check_blog_end_of_posts(db, con, blog):
    """Check if we have indeed done everything right"""

    logging.debug("{0} check_blog_end_of_posts".format(blog.name))

    if blog.posts_scraped >= blog.total_posts or blog.offset >= blog.total_posts:
        logging.info("Marking {0} as DONE".format(blog.name))
        blog.crawl_status = 'DONE'
        blog.offset = 0
    else:
        logging.info("Marking {0} as resume".format(blog.name))
        blog.crawl_status = 'resume'

    blog.crawling = 0

    if blog.posts_scraped == 0:
        blog.posts_scraped = db_handler.get_total_post(db, con, blog)
    return



def api_get_request_wrapper(db, con, lock, blog, update, crawl_status, offset=None):
    """Updates the update, valid or invalid"""

    # Retry getting /posts until either 404 or success
    update.__init__()
    attempts = 0
    while not update.valid and attempts < 3:
        attempts += 1
        try:
           blog.api_get_request(lock, update, api_key=None, reqtype="posts", offset=offset)
        except BaseException as e:
            traceback.print_exc()
            logging.error(BColors.RED + \
            "{0} Too many proxy attempts! Skipping for now. Error:{1}"\
            .format(blog.name, e) + BColors.ENDC)
            if crawl_status != 'resume':
                thread_premature_cleanup(db, con, blog, crawl_status)
                return False
            break
    return True


def blog_status_check(db, con, lock, blog, update, offset=None):
    """Returns True on update validated, otherwise false"""

    if blog.crawl_status == 'new':
        isnew = True
    else:
        isnew = False

    if blog.offset != blog.posts_scraped:
        logging.debug(BColors.DARKGRAY + "{0} Error: blog.offset={1} blog.posts_scraped={2}.\
Getting actual posts_scraped from DB".format(blog.name, blog.offset, blog.posts_scraped) + BColors.ENDC)
        blog.posts_scraped = db_handler.get_total_post(db, con, blog)
        logging.debug(BColors.DARKGRAY + "{0} Got {1} posts_scraped from DB".format(blog.name, blog.posts_scraped) + BColors.ENDC)

    api_get_request_wrapper(db, con, lock, blog, update, blog.crawl_status, offset=offset)

    if update.valid:
        # update and retrieve remaining blog info
        blog.total_posts = update.total_posts

        if blog.crawl_status is None or blog.crawl_status == 'new':
            blog.crawl_status = "resume"
        elif  blog.crawl_status == 'DONE':
            blog.crawl_status = None

        db_response = db_handler.update_blog_info(db, con, blog)

        logging.debug(BColors.BLUE + "{0} Got DB response: {1}"\
        .format(blog.name, db_response) + BColors.ENDC)

        if not check_db_init_response(db_response, blog, isnew=isnew):
            logging.debug("{0} check_db_init_response: False".format(blog.name))
            return False

    if not update.valid:
        logging.error(BColors.RED + "{0} Too many invalid request attempts! Aborting for now."\
        .format(blog.name) + BColors.ENDC)
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

            logging.error(BColors.FAIL + BColors.BOLD + \
            "{0} WARNING: number of posts has decreased from {1} to {2}!\
    Blog was recently updated {3}, previously checked on {4}\n\
    Check what happened, did the author remove posts!?"\
            .format(blog.name, db_response.get('last_total_posts'), \
            blog.total_posts, db_response.get('last_updated'), db_response.get('last_checked')))
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
        # FIXME we have new posts
        blog.db_response = db_handler.update_blog_info(database, con, blog)

    blog.total_posts = update.total_posts
    blog.last_updated = update.updated


def update_offset_if_new_posts(blog):
    """Increments offset if new posts arrived while scraping, according to header"""

    # to avoid re-inserting previously inserted posts
    if blog.new_posts > 0:
        blog.offset += blog.new_posts
        blog.new_posts = 0
        logging.info(BColors.RED + \
        "{0} Offset incremented because of new posts by +{1}: {2}".format(blog.name, blog.new_posts, blog.offset) + BColors.ENDC)


def blog_generator(db, con):
    """Queries DB for a blog that is either new or needs update.
    Returns a TumblrBlog() object instance with no proxy attached to it."""

    blog = TumblrBlog()
    blog.name, blog.offset, blog.health, blog.crawl_status, blog.total_posts, \
    blog.posts_scraped, blog.last_checked, blog.last_updated = db_handler.fetch_random_blog(db, con)

    if not blog.name:
        logging.debug(BColors.RED + "No blog fetched in blog_generator()" + BColors.ENDC)
        return blog

    if blog.offset is None:
        blog.offset = 0

    if blog.posts_scraped is None:
        blog.posts_scraped = 0

    # attach a proxy
    blog.attach_proxy()
    # init requests.session with headers
    blog.init_session()
    blog.attach_random_api_key()

    logging.info(BColors.CYAN + "{0} Got blog from DB."\
    .format(blog.name) + BColors.ENDC)
    logging.debug(BColors.CYAN + "{0}".format(blog.__dict__) + BColors.ENDC)

    return blog






class TumblrBlog:
    """blog object, holding retrieved values to pass along"""

    def __init__(self, *args):
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

    def init_session(self):
        if not self.requests_session: # first time
            requests_session = requests.Session()
            requests_session.headers.update(\
            {'User-Agent': self.proxy_object.get('user_agent')})
            requests_session.proxies.update(\
            {'http': self.proxy_object.get('ip_address'), \
            'https': self.proxy_object.get('ip_address')})
            self.requests_session = requests_session
        else:
            self.requests_session.headers.update(\
            {'User-Agent': self.proxy_object.get('user_agent')})
            self.requests_session.proxies.update(\
            {'http': self.proxy_object.get('ip_address'), \
            'https': self.proxy_object.get('ip_address')})


    def attach_proxy(self, proxy_object=None):
        """ attach proxy object, refresh session on update too"""

        if not proxy_object:  #common case
            proxy_object = next(instances.proxy_scanner.definitive_proxy_cycle)

        if not self.proxy_object: #first time
            self.proxy_object = proxy_object
        else: #we're updating
            self.proxy_object = proxy_object
            self.init_session() # refresh


    def attach_random_api_key(self):
        """ attach api key fetched from global list to proxy object already attached"""

        try:
            self.api_key_object_ref = api_keys.get_random_api_key(instances.api_keys)
            # print("APIKEY attached: {0}".format(self.api_key_object_ref.api_key))
            # self.proxy_object.api_key = temp_key.api_key
            # self.proxy_object.secret_key =  temp_key.secret_key
            # attach string to local proxy dict, in case we need to keep the proxy for later use
            self.proxy_object.update({'api_key': self.api_key_object_ref.api_key})
        except (AttributeError, BaseException):
            raise


    def renew_api_key(self, disable=True, old_api_key=None):
        # mark as disabled from global list pool
        if not old_api_key:
            old_api_key = self.api_key_object_ref

        if disable:
            api_keys.disable_api_key(old_api_key)

        self.attach_random_api_key()


    def get_new_proxy(self, lock, old_proxy_object=None):
        """ Pops old proxy gone bad from cycle, get a new one """
        if not old_proxy_object:
            old_proxy_object = self.proxy_object

        with lock: # get_new_proxy() requires a lock!
            self.proxy_object = instances.proxy_scanner.get_new_proxy(old_proxy_object)

        self.attach_random_api_key()
        self.init_session() # refresh session

        logging.info(BColors.BLUEOK + "{0} Changed proxy to {1}"\
        .format(self.name, self.proxy_object.get('ip_address')) + BColors.ENDC)



    def api_get_request(self, lock, updateobj, api_key=None, reqtype="posts", offset=None):
        """Returns requests.response object, reqype=[posts|info]"""
        if not api_key:
            api_key = self.api_key_object_ref
        if api_key.is_disabled():
            self.attach_random_api_key()

        if not offset or offset == 0:
            offset = ''
        else:
            offset = '&offset=' + str(offset)

        instances.sleep_here(0, 4)
        attempt = 0
        response = requests.Response()

        if not self.requests_session:
            self.init_session()
            logging.debug("---\n{0}Initializing new requests session: {1} {2}\n----"\
            .format(self.name, self.requests_session.proxies, self.requests_session.headers))

        apiv2_url = 'https://api.tumblr.com/v2/blog/{0}/{1}?api_key={2}{3}'\
        .format(self.name, reqtype, api_key.api_key, offset)

        while attempt < 10:
            try:
                logging.info(BColors.GREEN + BColors.BOLD +
                "{0} GET ip: {1} url: {2}".format(self.name,
                self.proxy_object.get('ip_address'), apiv2_url) + BColors.ENDC)

                response = self.requests_session.get(url=apiv2_url, timeout=10)

                api_keys.inc_key_request(api_key)

            except (requests.exceptions.ProxyError, requests.exceptions.Timeout) as e:
                logging.info(BColors.FAIL + "{0} Proxy error (continuing): {1}"\
                .format(self.name, e.__repr__()) + BColors.ENDC)

                self.get_new_proxy(lock)
                attempt += 1
                continue
            except (ConnectionError, requests.exceptions.RequestException) as e:
                logging.info(BColors.FAIL + "{0} Connection error (passing): {1}"\
                .format(self.name, e.__repr__()) + BColors.ENDC)
                # raise
                pass
            break

        try:
            self.check_response_validate_update(response, updateobj)
            return
        except:
            raise


    def check_response_validate_update(self, response, update):
        """ Reads the response object, updates the blog attributes accordingly.
        Last checks before updating BLOG table with info
        if unauthorized in response, change API key here, etc."""
        # TESTING:
        # update = parse_json_response(json.load(open\
        # (SCRIPTDIR + "/tools/test/videogame-fantasy_july_reblogfalse_dupe.json", 'r')))

        try:
            response_json = response.json()
        except (ValueError, json.decoder.JSONDecodeError):
            logging.exception(BColors.YELLOW
            + "{0} Error trying to parse response into json. Exerpt: {1}"
            .format(self.name, response.text[:1000]) + BColors.ENDC)

            try:
                response_json = response.text.split('''"response":{''')[1]
                response_json = r'{"meta": {"status": 200,"msg": "OK","x_tumblr_content_rating": "adult"},' + response_json
                logging.debug(BColors.YELLOW + "split: {0}"
                .format(response_json[:1000]) + BColors.ENDC)
                try:
                    response_json = response.json()
                except:
                    logging.debug(BColors.FAIL + "Fucking damnit can't get a good json!\n{0}"
                    .format(response_json) + BColors.ENDC)
                    raise
            except:
                response_json = {'meta': {'status': 500, 'msg': 'Server Error'},
            'response': [], 'errors': [{"title": "Malformed JSON or HTML was returned."}]}
        except:
            logging.exception(BColors.YELLOW
            + "{0} Fatal Error trying to get json from response: {1}"
            .format(self.name, response.text) + BColors.ENDC)
            raise


        logging.debug(BColors.LIGHTCYAN +
        "{0} Before parsing reponse check_response_validate_update response_json status={1} response_json msg {2}"\
        .format(self.name, response_json.get('meta').get('status'),
        response_json.get('meta').get('msg')) + BColors.ENDC)
        logging.debug(BColors.LIGHTCYAN + "{0} JSON is: {1}".format(self.name, str(response_json)[:1000]) + BColors.ENDC)

        update.meta_status = response_json.get('meta').get('status')
        update.meta_msg = response_json.get('meta').get('msg')


        if not response_json.get('response') or not (200 <= update.meta_status <= 399):
            logging.debug(BColors.BOLD + "{0} Got errors in Json! response: {1} meta_status: {2}"
            .format(self.name, response_json.get('response'), update.meta_status) + BColors.ENDC)

        # BIG PARSE (REMOVE?)
        resp_json = response_json.get('response')
        if resp_json is not None and resp_json != []:
            update.blogname = resp_json.get('blog', {}).get('name')
            update.total_posts = resp_json.get('blog', {}).get('total_posts')
            update.updated = resp_json.get('blog', {}).get('updated')
            update.posts_response = resp_json.get('posts', []) #list of dicts


        logging.debug(BColors.LIGHTCYAN +
        "{0} After parsing reponse, check_response_validate_update update.meta_msg={1} update.meta_status {2}"
        .format(self.name, update.meta_msg, update.meta_status) + BColors.ENDC)

        if response_json.get('errors') is not None:
            if update.meta_status == 404 or update.meta_msg.find('Not Found') != -1:
                logging.warning(BColors.FAIL + "{0} update has 404 error status {1} {2}! Setting to DEAD!"\
                .format(self.name, update.meta_status, update.meta_msg) + BColors.ENDC)
                self.health = "DEAD"
                self.crawl_status = "DEAD"
                update.valid = True
                return

            if update.errors_title.find("error") != -1 and\
                update.errors_title.find("Unauthorized") != -1:

                logging.critical(BColors.FAIL +
                "{0} is unauthorized! Rolling for a new API key.\n{1}"\
                .format(self.name, response_json) + BColors.ENDC)
                # FIXME: that's assuming only the API key is responsible for unauthorized, might be the IP!
                self.renew_api_key(disable=True)
                update.valid = False
                return

            logging.info(BColors.FAIL +
            "{0} uncaught error in response: {1}"
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

        return



class UpdatePayload(requests.Response):
    """ Container dictionary holding values from json to pass along """
    def __init__(self):
        self.errors_title = None
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
    """Main cleanup before exit"""
    logging.debug(BColors.LIGHTGRAY + \
    "final_cleanup, writing keys and proxies to json" + BColors.ENDC )
    proxy_scanner.write_proxies_to_json_on_disk()
    api_keys.write_api_keys_to_json()


def thread_good_cleanup(db, con, blog):
    """Cleanup when thread is terminated"""

    logging.debug(BColors.LIGHTGRAY + \
    "{0} thread_good_cleanup -> update_blog_info"\
    .format(blog.name) + BColors.ENDC )

    blog.crawling = 0
    # db_handler.update_crawling(db, con, blog)
    db_handler.update_blog_info(db, con, blog, ignore_response=True)


def thread_premature_cleanup(db, con, blog, reset_type):
    """Blog has not been updated in any way, just reset STATUS to NEW if was INIT"""

    logging.debug(BColors.LIGHTGRAY + \
    "{0} thread_premature_cleanup, reset_to_brand_new '{1}'"\
    .format(blog.name, reset_type) + BColors.ENDC )

    # only resets CRAWL_STATUS to 'new', not CRAWLING which stays 1 to avoid repicking it straight away
    db_handler.reset_to_brand_new(db, con, blog, reset_type='new')


def configure_logging(args):

    logging.basicConfig(format='{levelname}:    \t{message}', style='{',
                        level=getattr(logging, args.log_level.upper()))
    logger = logging.getLogger()

    instances.config = parse_config(args.config_path, args.data_path)

    fh = logging.FileHandler(filename=instances.config.get('tumblrmapper', 'log_path'),
                            mode='w')

    fh.setLevel(getattr(logging, instances.config.get('tumblrmapper', 'log_level')))
    fh.setFormatter(logging.Formatter(
                    '{asctime} {levelname}:{threadName}\t{message}',
                    '%y/%m/%d %H:%M:%S', '{'))
    logger.addHandler(fh)

    # sh = logging.StreamHandler(sys.stdout)
    # sh.setLevel(getattr(logging, args.log_level.upper()))
    # sh.setFormatter(logging.Formatter('{levelname}:\t{message}', None, '{'))
    # logger.addHandler(sh)

    logging.debug("Debugging Enabled.")
    return logger


def main(args):
    THREADS = instances.config.getint('tumblrmapper', 'threads')

    if args.create_blank_db or args.update_blogs or args.update_archives or args.update_archive_list:

        if args.update_archive_list:
            update_archive_lists.main(output_pickle=True)

        if args.create_blank_db:
            temp_database = db_handler.Database(
                db_filepath=instances.config.get('tumblrmapper', 'db_filepath')\
    + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                username="sysdba", password="masterkey")
            try:
                db_handler.create_blank_database(temp_database)
            except Exception as e:
                logging.error(BColors.FAIL + "Database creation failed:{0}"
                .format(e) + BColors.ENDC)

        if args.update_blogs:
            blogs_toscrape = instances.config.get('tumblrmapper', 'blogs_to_scrape')
            temp_database = db_handler.Database(
                db_filepath=instances.config.get('tumblrmapper', 'db_filepath')\
    + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                username="sysdba", password="masterkey")
            db_handler.populate_db_with_blogs(temp_database, blogs_toscrape)
            logging.error(BColors.BLUEOK + BColors.GREEN + "Done inserting blogs"
             + BColors.ENDC)

        if args.update_archives:
            archives_toload = instances.config.get('tumblrmapper', 'archives')
            temp_database = db_handler.Database(
                db_filepath=instances.config.get('tumblrmapper', 'db_filepath')\
    + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                username="sysdba", password="masterkey")
            if "pickle" in archives_toload:
                db_handler.update_db_with_archives(temp_database,
                archives_toload, use_pickle=True)
            else:
                db_handler.update_db_with_archives(temp_database,
                archives_toload, use_pickle=False)
            logging.error(BColors.BLUEOK + BColors.GREEN +
            "Done inserting archives" + BColors.ENDC)

        sys.exit(0)


    # === API KEY ===
    # list of APIKey objects
    instances.api_keys = api_keys.get_api_key_object_list(\
    SCRIPTDIR + os.sep + instances.config.get('tumblrmapper', 'api_keys'))

    # === PROXIES ===
    # Get proxies from free proxies site
    instances.proxy_scanner = proxies.ProxyScanner(instances.config.get('tumblrmapper', 'proxies'))
    fresh_proxy_dict = instances.proxy_scanner.get_proxies_from_internet()
    # print(fresh_proxy_dict)

    fresh_proxy_dict = {'proxies': [{'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36(KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '47.206.51.67:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36', 'disabled': False}, {'ip_address': '92.53.73.138:8118', 'user_agent': 'Mozilla/5.0 (Windows NT6.1; WOW64; rv:21.0) Gecko/20130331 Firefox/21.0', 'disabled': False}, {'ip_address': '45.77.247.164:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '80.211.4.187:8080', 'user_agent': 'Mozilla/5.0 (Microsoft Windows NT 6.2.9200.0); rv:22.0) Gecko/20130405 Firefox/22.0', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '66.82.123.234:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64;x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh;Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '52.164.249.198:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '47.206.51.67:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36', 'disabled':False}, {'ip_address': '92.53.73.138:8118', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130331 Firefox/21.0', 'disabled': False}, {'ip_address': '45.77.247.164:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '80.211.4.187:8080', 'user_agent': 'Mozilla/5.0 (Microsoft Windows NT 6.2.9200.0); rv:22.0) Gecko/20130405 Firefox/22.0', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '191.34.157.243:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36', 'disabled': False}, {'ip_address': '61.91.251.235:8080', 'user_agent': 'Opera/9.80 (Windows NT 5.1; U; zh-tw) Presto/2.8.131 Version/11.10', 'disabled': False}, {'ip_address': '41.190.33.162:8080', 'user_agent': 'Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36', 'disabled': False}, {'ip_address': '80.48.119.28:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '213.99.103.187:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36', 'disabled': False}, {'ip_address': '141.105.121.181:80', 'user_agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)', 'disabled': False}]}

    # Associate api_key to each proxy in fresh list
    #FIXME remove the two next lines; deprecated
    # instances.proxy_scanner.gen_list_of_proxy_objects(fresh_proxy_dict)
    instances.proxy_scanner.gen_proxy_cycle(fresh_proxy_dict.get('proxies')) # dictionaries

    # === DATABASE ===
    db = db_handler.Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath')
                            + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                            username=instances.config.get('tumblrmapper', 'username'),
                            password=instances.config.get('tumblrmapper', 'password'))
    con = db.connect()
    db_handler.update_crawling(db, con) # reset crawling values in DB
    db.close_connection(con)


    os.nice(instances.config.getint('tumblrmapper', 'nice_level'))
    # === BLOG ===

    worker_threads = []

    pill2kill = threading.Event()
    lock = threading.Lock()
    db_update_lock = threading.Lock()

    # Handle pressing ctrl+c on Linux?
    signal_handler = SignalHandler(pill2kill, worker_threads)
    signal.signal(signal.SIGINT, signal_handler)

    api_keys.threaded_buckets()

    # q = []
    t = threading.Thread(target=input_thread, args=(pill2kill,worker_threads))
    t.daemon = True
    t.start()
    worker_threads.append(t)

    for _ in range(0, THREADS):
        args = (db, lock, db_update_lock, pill2kill)
        t = threading.Thread(target=process, args=args)
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
    # db.close_connection()
    final_cleanup(instances.proxy_scanner)
    sys.exit(0)


if __name__ == "__main__":
    # window = curses.initscr()
    # window.nodelay(True)
    # curses.echo()
    # curses.cbreak()

    # parse command-line arguments
    args = parse_args()
    configure_logging(args)

    main(args)
