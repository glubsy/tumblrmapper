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
import db_handler
import instances
import proxies
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
    """ daemon threads setting pill2kill event when keyboard press occurs"""
    # DEBUG
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
        if threading.active_count() < 3:
            print(BColors.LIGHTYELLOW + "Less than 3 threads, exiting." + BColors.ENDC)
            event.set()
            return
        if input():
            event.set()

    # while True:
    #     if input():
    #         print(qlist)
    #         qlist.append(None)

def process(db, lock, pill2kill):

    con = db.connect()
    blog = None

    while not pill2kill.is_set():

        with lock:
            blog = blog_generator(db, con)

        if blog.name is None:
            print(BColors.FAIL + "No blog name fetched! No more to process?" + BColors.ENDC)
            break

        instances.sleep_here()

        blog.update = UpdatePayload()

        if blog.crawl_status == 'new' or blog.crawl_status == 'resume': # not yet updated

            # Retry getting /posts until either 404 or success
            attempts = 0
            while not blog.update.valid and attempts < 3:
                attempts += 1
                instances.sleep_here()
                # try:
                response = blog.api_get_request(lock, api_key=None, reqtype="posts")
                # try: 
                blog.check_response_validate_update(response)
                # except BaseException as e:
                #     print(BColors.RED + "Too many proxy attempts for {0}! Skipping for now. {1}".format(blog.name, e) + BColors.ENDC)
                #     thread_premature_cleanup(db, con, blog)
                #     break
                # except Exception as e :
                #     print("DEBUG STUB error in check_response(): {0}".format(e))

            if blog.update.valid:
                # update and retrieve remaining blog info
                db_response = db_handler.update_blog_info(db, con, blog, init=True) # tuple
                print("response: {0} {1}".format(type(db_response), db_response))
                check_db_init_response(db_response, blog)

            if not blog.update.valid:
                print(BColors.RED + "Too many invalid request attempts for {0}! Aborting."\
                .format(blog.name) + BColors.ENDC)
                thread_premature_cleanup(db, con, blog)
                return

        if blog.health == 'DEAD':
            print(BColors.LIGHTRED + "WARNING! Blog {0} appears to be dead!".format(blog.name) + BColors.ENDC)
            continue


        # STARTING BUSINESS
        if pill2kill.is_set():
            break

        # we already have the first batch of posts, insert them
        if blog.offset is None: # DB had no previous offset, it's brand new
            blog.offset = 0
            processed_posts = db_handler.insert_posts(db, con, blog)
            blog.posts_scraped += processed_posts
            blog.offset += blog.posts_scraped
            print("OFFSETS are now: {0}".format(blog.offset))


        while not pill2kill.is_set():
            if not blog.update.trimmed_posts_list: #get more
                blog.api_get_request(lock, api_key=None, reqtype="posts", offset=blog.offset)

                blog.offset += 20
                if not blog.update.trimmed_posts_list: # nothing more
                    break

        if pill2kill.is_set():
            break
        

    print(BColors.LIGHTGRAY + "Terminating thread {0}"\
    .format(threading.current_thread()) + BColors.ENDC)
    if blog is not None:
        thread_good_cleanup(db, con, blog)
    return


def check_db_init_response(db_response, blog):
    """Parse items returned by DB on blog update"""

    if not db_response:
        return

    if db_response.get('last_total_posts', 0) < blog.total_posts:
        print(BColors.BOLD + "Blog {0} has been updated. Old total_posts {1}, new total_posts {2}."\
        .format(blog.name, db_response.get('last_total_posts'), blog.total_posts) + BColors.ENDC)

    elif db_response.get('last_total_posts', 0) > blog.total_posts:
        print(BColors.FAIL + BColors.BOLD + "WARNING: number of posts for {0} has decreased from {1} to {2}!\
 Blog was recently updated {3}, previously checked on {4}\n Check what happened, did the author remove posts!?"\
        .format(blog.name, db_response.get('last_total_posts'), blog.total_posts, db_response.get('last_updated'), db_response.get('last_checked')))

    blog.offset = db_response.get('last_offset', 0)
    blog.post_scraped = db_response.get('last_scraped_posts')
    blog.crawling = 1 # set by DB by procedure on its side


def crawler(blog, offset=None):
    print("start crawling {0}, offset: {1}".format(blog.name, offset))
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
    blog.name, blog.offset, blog.health, blog.crawl_status, blog.total_posts, \
    blog.post_scraped, blog.last_checked, blog.last_updated = db_handler.fetch_random_blog(db, con)

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






class TumblrBlog:
    """blog object, holding retrieved values to pass along"""

    def __init__(self):
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
        self.response = None

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
    def requester(self, url, requests_session=None, api_key=None):
        """ Does a request, returns json """
        # url = 'https://httpbin.org/get'

        if not requests_session:
            self.init_session()
            print("---\nRequested new session for {0}: {1} {2}\n----"\
            .format(self.name, self.requests_session.proxies, self.requests_session.headers))

        print(BColors.GREEN + "Getting: {0}".format(url) + BColors.ENDC)
        try:
            response = self.requests_session.get(url, timeout=10)
            # if response.status_code == 200:
            if response.status_code > 200:
                api_keys.inc_key_request(api_key)
                # json_data = response.json()
            return response
        except:
            raise


    def api_get_request(self, lock, api_key=None, reqtype="posts", offset=None):
        """Returns requests.response object, reqype=[posts|info]"""
        if not api_key:
            api_key = self.api_key_object_ref
        if not offset:
            offset = ''
        else:
            offset = '&offset=' + str(offset)
        instances.sleep_here(2, 6)
        attempt = 0
        apiv2_url = 'https://api.tumblr.com/v2/blog/{0}/{1}?api_key={2}{3}'.format(self.name, reqtype, api_key.api_key, offset)
        print(BColors.BOLD + "api_get_request({0}): proxy: {1}".format(self.name, self.proxy_object.get('ip_address')) + BColors.ENDC)
        # renew proxy n times if it fails
        while attempt < 10:
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


    def check_response_validate_update(self, response):
        """ Reads the response object, updates the blog attributes accordingly.
        Last checks before updating BLOG table with info
        if unauthorized in response, change API key here, etc."""
        # TESTING:
        # update = parse_json_response(json.load(open\
        # (SCRIPTDIR + "/tools/test/videogame-fantasy_july_reblogfalse_dupe.json", 'r')))

        try:
            self.response = response.json()
        except ValueError:
            print(BColors.YELLOW + "Error trying to get json from response for blog {0}".format(self.name) + BColors)
            raise

        print(BColors.GREEN + "check_response({0}) response={1}".format(self.name, self.response.get('meta')['status']) + BColors.ENDC)

        parse_json_response(self.response, self.update)

        print(BColors.LIGHTCYAN + "check_response({0}) update={1}".format(self.name, self.update.meta_msg) + BColors.ENDC)

        if self.update.errors_title is not None:
            if self.update.meta_status == 404 and self.update.meta_msg == 'Not Found':
                print(BColors.FAIL + "Blog {0}: is apparently DEAD.".format(self.name) + BColors.ENDC)
                self.health = "DEAD"
                self.crawl_status = "DEAD"
                self.total_posts = 0
                self.last_updated = 0
                self.update.valid = True
                return

            if self.update.errors_title.find("error") and self.update.errors_title.find("Unauthorized"):
                print(BColors.FAIL + "Blog {0}: is unauthorized! Missing API key? REROLL!".format(self.name) + BColors.ENDC)
                # FIXME: that's assuming only the API key is responsible for unauthorized, might be the IP!
                self.renew_api_key()
                self.update.valid = False
                return

            print(BColors.FAIL + "Blog {0}: uncaught error in response: {1}".format(self.name, self.update.__dict__) + BColors.ENDC)
            self.update.valid = False
            return

        self.update.valid = True
        self.health = "UP"
        self.total_posts = self.update.total_posts
        self.last_updated = self.update.updated
        self.crawl_status = "resume"
        self.crawling = 1

        if self.update.total_posts < 20:   #FIXME: arbitrary value
            print("check_response() Warning: {0} is considered WIPED.".format(self.name))
            self.health = "WIPED"

        print(BColors.BLUEOK + "No error in check_response() json for {0}".format(self.name) + BColors.ENDC)

        return



class UpdatePayload(requests.Response):
    """ Container dictionary holding values from json to pass along """
    def __init__(self):
        self.errors_title = None
        self.valid = False
        self.meta_status = None
        self.meta_msg = None
        self.total_posts = None
        self.trimmed_posts_list = None
        self.updated = None


def parse_json_response(response_json, update):
    """returns a UpdatePayload() object that holds the fields to update in DB"""
    t0 = time.time()

    update.meta_status = response_json.get('meta')['status']
    update.meta_msg = response_json.get('meta')['msg']

    # if not 200 <= json['meta']['status'] <= 399:
    #     update.errors = json['errors']
    #     return update

    if not response_json.get('response') and response_json.get('errors'): # got errors!
        update.errors_title = response_json.get('errors')[0]['title']
        return 

    json = response_json.get('response')
    update.blogname = json['blog']['name']
    update.total_posts = json['blog']['total_posts']
    update.updated = json['blog']['updated']
    if json.get('posts'):
        update.posts_response = json['posts'] #list of dicts
        update.trimmed_posts_list = [] #list of dicts of posts

        for post in update.posts_response: #dict in list
            current_post_dict = {}
            current_post_dict['id'] = post.get('id')
            current_post_dict['date'] = post.get('date')
            current_post_dict['updated'] = post.get('updated')
            current_post_dict['post_url'] = post.get('post_url')
            current_post_dict['blog_name'] = post.get('blog_name')
            current_post_dict['timestamp'] = post.get('timestamp')
            if 'trail' in post.keys() and len(post['trail']) > 0: # trail is not empty, it's a reblog
                #FIXME: put this in a trail subdictionary
                current_post_dict['reblogged_blog_name'] = post['trail'][0]['blog']['name']
                current_post_dict['remote_id'] = int(post['trail'][0]['post']['id'])
                current_post_dict['remote_content'] = post['trail'][0]['content_raw'].replace('\n', '')
            else: #trail is an empty list
                current_post_dict['reblogged_blog_name'] = None
                current_post_dict['remote_id'] = None
                current_post_dict['remote_content'] = None
                pass #FIXME: maybe problematic for the following, might get skipped
            current_post_dict['photos'] = []
            if 'photos' in post.keys():
                for item in range(0, len(post['photos'])):
                    current_post_dict['photos'].append(post['photos'][item]['original_size']['url'])

            update.trimmed_posts_list.append(current_post_dict)

        t1 = time.time()
        print('Building list of posts took %.2f ms' % (1000*(t1-t0)))

#     for post in update.trimmed_posts_list:
#         print("===============================\n\
# POST number: " + str(update.trimmed_posts_list.index(post)))
#         for key, value in post.items():
#             print("key: " + str(key) + "\nvalue: " + str(value) + "\n--")

    return update



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
    proxy_scanner.write_proxies_to_json_on_disk()
    api_keys.write_api_keys_to_json()


def thread_good_cleanup(db, con, blog):
    """Cleanup when thread is terminated"""
    blog.crawling = 0
    # db_handler.update_crawling_status(db, con, blog)
    db_handler.update_blog_info(db, con, blog)

def thread_premature_cleanup(db, con, blog):
    """Blog has not been updated in any way, just reset STATUS to NEW"""
    db_handler.reset_to_brand_new(db, con, blog)

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

    print(BColors.LIGHTGRAY + "Done with all threads" + BColors.ENDC)

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
    # sys.exit(0)

    # try:
    #     main()
    # except Exception as e:
    #     logging.debug("Error in main():" + str(e))
