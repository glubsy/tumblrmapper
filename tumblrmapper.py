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
from concurrent import futures
from itertools import cycle

import requests

import api_keys
from api_keys import count_api_requests
import db_handler
import instances
import proxies
import tumblr_client
import tumdlr_classes
from constants import BColors
# import curses
# import ratelimit


# try:
#     from tqdm import tqdm
#     TQDM_AVAILABLE = True
# except ImportError:
#     TQDM_AVAILABLE = False
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
    parser.add_argument('-l', '--log_level', action="store", default="INFO",
                        help="Set log level: DEBUG, INFO (default), WARNING, ERROR, CRITICAL")

    parser.add_argument('-p', '--proxies', action="store_true", default=False,
                        help="Use randomly selected proxies")
    parser.add_argument('-v', '--api_version', action="store", type=int, default=2,
                        help="API version to query, default is 2.")
    parser.add_argument('-b', '--blog_list', action="store",
                        help="Path to initial blog list to populate DB")
    parser.add_argument('-s', '--archive_list', action="store",
                        help="Path to initial archive list to populate DB")
    parser.add_argument('-n', '--create_blank_db', action="store_true",
                        help="Create a blank DB in data_dir and populate it")

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
                        "scraper_log": data_path + os.sep + "scraper.log", #log for downloads and proxies
                        "blank_db": data_path + os.sep + "blank_db.fdb", #blank initial DB file
                        "db_filepath": data_path + os.sep, #blank initial DB file
                        "api_version": "2", #use api v2 by default
                        "use_proxies": False, #use random proxies, or not
                        "api_keys": data_path + os.sep + "api_keys.json",
                        "proxies": data_path + os.sep + "proxies.json",
                        "threads": 10
                        }

    config = configparser.SafeConfigParser(config_defaults)
    config.add_section('tumblrmapper')

    # Try to read config file (either passed in, or default value)
    # conf_file = os.path.join(config.get('tumblrmapper', 'config_path'), 'config')
    logging.debug("Trying to read config file: %s", config_path)
    result = config.read(config_path)
    if not result:
        logging.debug("Unable to read config file: %s", config_path)

    config.set('tumblrmapper', 'blogs_to_scrape', SCRIPTDIR + os.sep + config.get('tumblrmapper', 'blogs_to_scrape'))
    config.set('tumblrmapper', 'archives', SCRIPTDIR + os.sep + config.get('tumblrmapper', 'archives'))
    config.set('tumblrmapper', 'db_filepath', os.path.expanduser(config.get('tumblrmapper', 'db_filepath')))
    config.set('tumblrmapper', 'blank_db', os.path.expanduser(config.get('tumblrmapper', 'data_path') + os.sep + "blank_db.fdb"))

    logging.debug("Merged config: %s",
                sorted(dict(config.items('tumblrmapper')).items()))

    return config



def input_thread(event, worker_threads):
    # while True:
    #     if input():
    #         print(q)
    #         q.append(None)
    
    # attempt = 0
    # while attempt < 10:
    #     attempt += 1
    #     print(BColors.BOLD + "Length of worker threads list: {0}, alive: {1}, list: {2}, current {3}"\
    #     .format(len(worker_threads), threading.active_count(), threading.enumerate(), threading.current_thread()) + BColors.ENDC)
    #     for thread in threading.enumerate():
    #         print(BColors.BOLD + "thread: {0} daemon: {1}".format(thread, thread.daemon) + BColors.ENDC)
    #     print("\n")
    #     time.sleep(1)

    while not event.is_set():
        if threading.active_count() < 4:
            print(BColors.LIGHTYELLOW + "Less than 3 threads, exiting." + BColors.ENDC)
            event.set()
            return
        if input():
            event.set()


                


def process(db, lock, pill2kill):

    con = db.connect()

    while not pill2kill.is_set():

        with lock:
            blog = blog_generator(db, con)

        if blog.name is None:
            print(BColors.FAIL + "No blog name fetched! No more to process?" + BColors.ENDC)
            break

        instances.sleep_here()

        update = tumblr_client.UpdatePayload()

        if blog.crawl_status == 'new' or blog.crawl_status == 'resume': # not yet updated
            attempts = 0
            while not update.valid and attempts < 3:
                attempts += 1
                instances.sleep_here()
                # try:
                response = blog.api_get_blog_json(lock, api_key=None, reqtype="info")
                check_response(blog, response, update)
                # except Exception as e :
                #     print("DEBUG STUB error in check_response(): {0}".format(e))

            if update.valid:
                # update and retrieve blog info
                db_response = db_handler.update_blog_info(db, con, blog) # tuple
                print("response: {0} {1}".format(type(db_response), db_response))
                check_db_init_response(db_response, blog)
                # db_blog_record =
                # offset = read_offset_from_DB(db_blog_record)

        if not update.valid:
            print(BColors.RED + "Too many attemps for {0}! Aborting.".format(blog.name) + BColors.ENDC)
            return

        if blog.health == 'DEAD':
            print(BColors.LIGHTRED + "WARNING! Blog {0} appears to be dead!".format(blog.name) + BColors.ENDC)
            continue

        if pill2kill.is_set():
            thread_cleanup(db, con, blog)
            print("BREAK")
            break

        crawler(blog)

        if pill2kill.is_set():
            thread_cleanup(db, con, blog)
            print("BREAK")
            break

    print("TERMINATED THREAD")
    return




def thread_cleanup(db, con, blog):
    """ before quitting, need cleanup"""
    blog.crawling = 0
    db_handler.update_crawling_status(db, con, blog)




def read_offset_from_DB(blog):
    # STUB
    return None


def check_db_init_response(db_response, blog):

    if not db_response[0]:
        return

    if db_response[0] > blog.total_posts:
        print(BColors.FAIL + "WARNING: number of posts for {0} has decreased from {1} to {2}!\
 Blog was recently updated {3}, previously checked on {4}"\
        .format(blog.name, db_response[0], blog.total_posts, db_response[1], db_response[2] ))

    blog.crawling = 1


def crawler(blog, offset=None):
    print("start crawling {0}, offsert: {1}".format(blog.name, offset))
    return
    # update_crawl_status(blog, 1)
    # crawl(blog)


# def worker_blog_queue_feeder():
#     """ Keeps trying to get a new blog to put in the queue blog_object_queue"""
#     isfull = None
#     while not isfull:
#         try:
#             isfull = blog_object_queue.put(blog_generator())
#         except:
#             pass



def blog_generator(db, con):
    """Queries DB for a blog that is either new or needs update.
    Returns a TumblrBlog() object instance with no proxy attached to it."""

    blog = TumblrBlog()
    blog.name, blog.total_posts, blog.health, \
    blog.crawl_status, blog.post_scraped, \
    blog.offset, blog.last_updated = db_handler.fetch_random_blog(db, con)

    if not blog.name:
        print(BColors.FAIL + "No blog fetched in blog_generator()!" + BColors.ENDC)
        return blog

    # attach a proxy
    blog.attach_proxy()
    # init requests.session with headers
    blog.init_session()
    blog.attach_random_api_key()

    print(BColors.CYAN + "Got blog from DB: {0}".format(blog.name) + BColors.ENDC)

    return blog


def check_response(blog, response, update):
    """ Reads the response object, updates the blog attributes accordingly.
    Last checks before updating BLOG table with info
    if unauthorized in response, change API key here, etc."""

    print(BColors.GREEN + "check_response({0}) response={1}".format(blog.name, response) + BColors.ENDC)
    # TESTING:
    # update = parse_json_response(json.load(open\
    # (SCRIPTDIR + "/tools/test/videogame-fantasy_july_reblogfalse_dupe.json", 'r')))

    tumblr_client.parse_json_response(response, update)
    print(BColors.LIGHTCYAN + "check_response({0}) update={1}".format(blog.name, update.__dict__) + BColors.ENDC)

    if update.errors_title is not None:
        if update.meta_status == 404 and update.meta_msg == 'Not Found':
            print(BColors.FAIL + "Blog {0}: is apparently DEAD.".format(blog.name) + BColors.ENDC)
            blog.health = "DEAD"
            blog.crawl_status = "DEAD"
            blog.total_posts = 0
            blog.last_updated = 0
            update.valid = True
            return update

        if "error" in update.errors_title and "Unauthorized" in update.errors_title:
            print(BColors.FAIL + "Blog {0}: is unauthorized! Missing API key? REROLL!".format(blog.name) + BColors.ENDC)
            # FIXME: that's assuming only the API key is responsible for unauthorized, might be the IP!
            blog.renew_api_key()
            update.valid = False
            return update

        print(BColors.FAIL + "Blog {0}: uncaught error in response: {1}".format(blog.name, update.__dict__) + BColors.ENDC)
        update.valid = False
        return update

    update.valid = True
    blog.health = "UP"
    blog.total_posts = update.total_posts
    blog.last_updated = update.updated
    blog.crawl_status = "resume"
    blog.crawling = 1

    if update.total_posts < 20:   #FIXME: arbitrary value
        print("check_response() Warning: {0} is considered WIPED.".format(blog.name))
        blog.health = "WIPED"

    print(BColors.BLUEOK + "No error in check_response() json for {0}".format(blog.name) + BColors.ENDC)

    return update



class TumblrBlog:
    """blog object, holding retrieved values to pass along"""

    def __init__(self):
        self.name = None
        self.total_posts = 0
        self.post_scraped = 0
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

    def init_session(self):
        if not self.requests_session: # first time
            requests_session = requests.Session()
            requests_session.headers.update({'User-Agent': self.proxy_object.get('user_agent')})
            requests_session.proxies.update({'http': self.proxy_object.get('ip_address'), 'https': self.proxy_object.get('ip_address')})
            self.requests_session = requests_session
        else:
            self.requests_session.headers.update({'User-Agent': self.proxy_object.get('user_agent')})
            self.requests_session.proxies.update({'http': self.proxy_object.get('ip_address'), 'https': self.proxy_object.get('ip_address')})


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

        self.api_key_object_ref = api_keys.get_random_api_key(instances.api_keys)
        # print("APIKEY attached: {0}".format(self.api_key_object_ref.api_key))
        # self.proxy_object.api_key = temp_key.api_key
        # self.proxy_object.secret_key =  temp_key.secret_key

        # attach string to local proxy dict, in case we need to keep the proxy for later use
        self.proxy_object.update({'api_key': self.api_key_object_ref.api_key})


    def renew_api_key(self, old_api_key=None):
        # mark as disabled from global list pool
        if not old_api_key:
            old_api_key = self.api_key_object_ref

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

        print(BColors.BLUEOK + "Changed proxy for {0} to {1}"\
        .format(self.name, self.proxy_object.get('ip_address')) + BColors.ENDC)


    # @ratelimit(1000, 3600)
    @api_keys.count_api_requests
    def requester(self, url, requests_session=None, api_key=None):
        """ Does a request, returns json """
        # url = 'https://httpbin.org/get'

        if not requests_session:
            self.init_session()
            print("---\nRequested new session for {0}: {1} {2}\n----"\
            .format(self.name, self.requests_session.proxies, self.requests_session.headers))

        print(BColors.GREEN + "Getting:{0}".format(url) + BColors.ENDC)
        try:

            response = self.requests_session.get(url, timeout=10)
            # if response.status_code == 200:
            if response.status_code > 0:
                json_data = response.json()
                return json_data
            return response.json() #FIXME: TESTING
        except:
            raise


    def api_get_blog_json(self, lock, api_key=None, reqtype="posts"):
        """Returns requests.response object, reqype=[posts|info]"""
        if not api_key:
            api_key = self.api_key_object_ref
        attempt = 0
        apiv2_url = 'https://api.tumblr.com/v2/blog/{0}/{1}?api_key={2}'.format(self.name, reqtype, api_key.api_key)
        print(BColors.bold + "api_get_blog_json({0}): proxy: {1}".format(self.name, self.proxy_object.get('ip_address')) + BColors.ENDC)
        # renew proxy n times if it fails
        while attempt < 3:
            try:
                response = self.requester(url=apiv2_url, requests_session=self.requests_session, api_key=api_key)
            except requests.exceptions.ProxyError as e:
                logging.debug(BColors.FAIL + "Proxy error for {0}: {1}"\
                .format(self.name, e.__repr__()) + BColors.ENDC)
                self.get_new_proxy(lock)
                attempt += 1
                continue
            except requests.exceptions.Timeout as e:
                logging.debug(BColors.FAIL + "Proxy Timeout on {0}: {1}"\
                .format(self.name, e.__repr__()) + BColors.ENDC )
                self.get_new_proxy(lock)
                attempt += 1
                continue
            return response
        return None




class SignalHandler:
    """The object that will handle signals and stop the worker threads.
    https://christopherdavis.me/blog/threading-basics.html"""
    stopper = None
    workers = None

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
    proxy_scanner.write_proxies_to_json_on_disk()
    api_keys.write_api_keys_to_json()


if __name__ == "__main__":
    # window = curses.initscr()
    # window.nodelay(True)
    # curses.echo()
    # curses.cbreak()

    # parse command-line arguments
    args = parse_args()

    logging.basicConfig(format='%(levelname)s:%(message)s',
                        level=getattr(logging, args.log_level.upper()))
    logging.debug("Debugging Enabled.")

    instances.config = parse_config(args.config_path, args.data_path)

    THREADS = instances.config.getint('tumblrmapper', 'threads')

    if args.create_blank_db: # we asked for a brand new DB file
        blogs_toscrape = instances.config.get('tumblrmapper', 'blogs_to_scrape')
        archives_toload = instances.config.get('tumblrmapper', 'archives')
        temp_database_path = instances.config.get('tumblrmapper', 'blank_db')
        temp_database = db_handler.Database(db_filepath=temp_database_path, \
                                username="sysdba", password="masterkey")

        db_handler.create_blank_database(temp_database)
        db_handler.populate_db_with_blogs(temp_database, blogs_toscrape)
        # Optional archives too
        # db_handler.populate_db_with_archives(temp_database, archives_toload)
        print(BColors.BLUEOK + BColors.GREEN + "Done creating blank DB in: " + temp_database.db_filepath + BColors.ENDC)
        sys.exit(0)


    # === API KEY ===
    # list of APIKey objects
    instances.api_keys = api_keys.get_api_key_object_list(\
    SCRIPTDIR + os.sep + instances.config.get('tumblrmapper', 'api_keys'))

    # === PROXIES ===
    # Get proxies from free proxies site
    instances.proxy_scanner = proxies.ProxyScanner(instances.config.get('tumblrmapper', 'proxies'))
    # fresh_proxy_dict = instances.proxy_scanner.get_proxies_from_internet()
    # print(fresh_proxy_dict)

    fresh_proxy_dict = {'proxies': [{'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36(KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '47.206.51.67:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36', 'disabled': False}, {'ip_address': '92.53.73.138:8118', 'user_agent': 'Mozilla/5.0 (Windows NT6.1; WOW64; rv:21.0) Gecko/20130331 Firefox/21.0', 'disabled': False}, {'ip_address': '45.77.247.164:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '80.211.4.187:8080', 'user_agent': 'Mozilla/5.0 (Microsoft Windows NT 6.2.9200.0); rv:22.0) Gecko/20130405 Firefox/22.0', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '66.82.123.234:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64;x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh;Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '52.164.249.198:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '47.206.51.67:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36', 'disabled':False}, {'ip_address': '92.53.73.138:8118', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130331 Firefox/21.0', 'disabled': False}, {'ip_address': '45.77.247.164:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '80.211.4.187:8080', 'user_agent': 'Mozilla/5.0 (Microsoft Windows NT 6.2.9200.0); rv:22.0) Gecko/20130405 Firefox/22.0', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '61.216.96.43:8081', 'user_agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', 'disabled': False}, {'ip_address': '185.119.56.8:53281', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', 'disabled': False}, {'ip_address': '191.34.157.243:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36', 'disabled': False}, {'ip_address': '61.91.251.235:8080', 'user_agent': 'Opera/9.80 (Windows NT 5.1; U; zh-tw) Presto/2.8.131 Version/11.10', 'disabled': False}, {'ip_address': '41.190.33.162:8080', 'user_agent': 'Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36', 'disabled': False}, {'ip_address': '80.48.119.28:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', 'disabled': False}, {'ip_address': '213.99.103.187:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36', 'disabled': False}, {'ip_address': '141.105.121.181:80', 'user_agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)', 'disabled': False}]}

    # Associate api_key to each proxy in fresh list
    #FIXME remove the two next lines; deprecated
    # instances.proxy_scanner.gen_list_of_proxy_objects(fresh_proxy_dict)
    instances.proxy_scanner.gen_proxy_cycle(fresh_proxy_dict.get('proxies')) # dictionaries

    # === DATABASE ===
    db = db_handler.Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath'), \
                            username=instances.config.get('tumblrmapper', 'username'),
                            password=instances.config.get('tumblrmapper', 'password'))
    con = db.connect()
    db_handler.update_crawling_status(db, con) # resets all crawling to 0
    db.close_connection(con)

    # === BLOG ===
    que = queue.Queue(maxsize=THREADS)


    worker_threads = []

    pill2kill = threading.Event()
    lock = threading.Lock()

    # Handle pressing ctrl+c on Linux?
    signal_handler = SignalHandler(pill2kill, worker_threads)
    signal.signal(signal.SIGINT, signal_handler)

    api_keys.threaded_buckets()

    q = []
    t = threading.Thread(target=input_thread, args=(pill2kill,worker_threads))
    t.daemon = True
    t.start()
    worker_threads.append(t)

    

    for i in range(0, THREADS):
        args = (db, lock, pill2kill)
        t = threading.Thread(target=process, args=args)
        worker_threads.append(t)
        # t.daemon = True
        t.start()

    # start incrementing API keys's buckets of tokens

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
    
    print(BColors.LIGHTPINK + "Done with all threads" + BColors.ENDC)

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
    if pill2kill.is_set():
        print("Main() exiting now.")
        final_cleanup(instances.proxy_scanner)
        sys.exit(0)

    # try:
    #     main()
    # except Exception as e:
    #     logging.debug("Error in main():" + str(e))
