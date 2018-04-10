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
from concurrent import futures
from itertools import cycle

import requests

import api_keys
import db_handler
import instances
import proxies
import tumblr_client
import tumdlr_classes
from constants import BColors

# import ratelimit


# try:
#     from tqdm import tqdm
#     TQDM_AVAILABLE = True
# except ImportError:
#     TQDM_AVAILABLE = False
SCRIPTDIR = os.path.dirname(__file__)
THREADS = 10


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
                        "proxies": False, #use random proxies, or not
                        "api_keys": data_path + os.sep + "api_keys.json",
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


def main():
    """Entry point"""
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
        return

    # === API KEY ===
    # list of APIKey objects
    instances.api_keys = api_keys.get_api_key_object_list(\
    SCRIPTDIR + os.sep + instances.config.get('tumblrmapper', 'api_keys'))

    # === PROXIES ===
    # Get proxies from free proxies site
    instances.proxy_scanner = proxies.ProxyScanner()
    # fresh_proxy_dict = instances.proxy_scanner.get_proxies_from_internet()
    # print(fresh_proxy_dict)

    fresh_proxy_dict = {'proxies': [{'ip_address': '92.53.73.138:8118', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1467.0 Safari/537.36', 'disabled': False}, {'ip_address': '162.242.158.97:3128', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}, {'ip_address': '92.47.30.74:45618', 'user_agent': 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR2.0.50727; InfoPath.2)', 'disabled': False}, {'ip_address': '122.183.243.68:8080', 'user_agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; sv-SE) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4', 'disabled': False}, {'ip_address': '191.102.92.187:80', 'user_agent': 'Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20130401 Firefox/21.0', 'disabled': False}, {'ip_address': '185.19.176.237:53281', 'user_agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.517 Safari/537.36', 'disabled': False}, {'ip_address': '158.69.200.254:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36', 'disabled': False}, {'ip_address': '192.116.142.153:8080', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36', 'disabled': False}, {'ip_address': '201.64.157.242:80', 'user_agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; ko-kr) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27', 'disabled': False}, {'ip_address': '89.236.17.106:3128', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36', 'disabled': False}, {'ip_address': '128.199.198.79:8118', 'user_agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML,like Gecko) Chrome/28.0.1467.0 Safari/537.36', 'disabled': False}, {'ip_address': '42.104.84.106:8080', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36', 'disabled': False}]}


    # Associate api_key to each proxy in fresh list
    #FIXME remove the two next lines; deprecated
    # instances.proxy_scanner.gen_list_of_proxy_objects(fresh_proxy_dict)
    instances.proxy_scanner.gen_proxy_cycle(fresh_proxy_dict.get('proxies'))
    print(next(instances.proxy_scanner.definitive_proxy_cycle))
    print(type(next(instances.proxy_scanner.definitive_proxy_cycle)))

    # === DATABASE ===
    db = db_handler.Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath'), \
                            username=instances.config.get('tumblrmapper', 'username'),
                            password=instances.config.get('tumblrmapper', 'password'))
    # db.connect()

    # === BLOG ===
    blog_object_queue = queue.Queue(maxsize=THREADS)

    # while True:
    # == Fetch 10 available blogs from DB ==
    daemon_threads = []
    worker_threads = []
    lock = threading.Lock()
    for i in range(0,THREADS):
        args = (db, lock)
        t = threading.Thread(target=process, args=args)
        worker_threads.append(t)
        t.start()

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


def process(db, lock):
    con = db.connect()
    with lock:
        blog = blog_generator(db, con)

    instances.sleep_here()

    update = UpdatePayload()

    if blog.crawl_status == 'new': # not yet updated

        while not update.valid:
            instances.sleep_here()
            try:
                response = blog.api_get_blog_json_health(lock)
                check_response(db, con, response, update)
            except:
                print("DEBUG STUB error in check_response()")

        # update and retrieve blog info
        db_handler.update_blog_info(blog, con, update)
        # db_blog_record = 
        # offset = db_blog_record['offset']

    elif blog.crawl_status == 'resume': # refresh health info

        while not update.valid:
            instances.sleep_here()
            try:
                response = blog.api_get_blog_json_health(lock)
                check_response(db, con, response, update)
            except:
                print("DEBUG STUB error in check_response()")

        # update and retrieve blog info
        db_handler.update_blog_info(blog, con, update)
        # db_blog_record = 
        # offset = read_offset_from_DB(db_blog_record)


    # if db_blog_record.health == 'DEAD':
    #     print(BColors.FAIL + "WARNING! Blog {0} appears to be dead!".format(db_blog_record.name) + BColors.ENDC)
    #     return
    startcrawling(blog, offset=offset)


def read_offset_from_DB(blog):
    # STUB
    return None


def startcrawling(blog, offset=None):
    print("start crawling {0}, offsert: {1}".format(blog.name, offset))
    return
    # update_crawl_status(blog, 1)
    # crawl(blog)


def worker_blog_queue_feeder():
    """ Keeps trying to get a new blog to put in the queue blog_object_queue"""
    isfull = None
    while not isfull:
        try:
            isfull = blog_object_queue.put(blog_generator())
        except:
            pass


def count_api_requests(func):
    def func_wrapper(*args, **kwargs):
        api_key = kwargs.get("api_key")
        api_key.use_once()
        print(BColors.LIGHTPINK + "API key used: {0}. Number of request: {1}"\
        .format(api_key, api_key.request_num) + BColors.ENDC)
    return func_wrapper


def blog_generator(db, con):
    """Queries DB for a blog that is either new or needs update.
    Returns a TumblrBlog() object instance with no proxy attached to it."""

    blog = TumblrBlog()
    blog.name, blog.total_posts, blog.health, \
    blog.crawl_status, blog.post_scraped, \
    blog.offset, blog.last_checked = db_handler.fetch_random_blog(db, con)

    # attach a proxy
    blog.attach_proxy(next(instances.proxy_scanner.definitive_proxy_cycle))
    # init requests.session with headers
    blog.init_session()
    blog.attach_random_api_key()

    print(BColors.CYAN + "Got blog from DB: {0}".format(blog.name) + BColors.ENDC)

    return blog


def check_response(blog, con, response, update):
    """ Reads the response object, updates the blog attributes accordingly.
    Last checks before updating BLOG table with info
    if unauthorized in response, change API key here, etc."""

    print(BColors.GREEN + "Updating DB with:", str(response) + BColors.ENDC)
    # TESTING:
    # update = parse_json_response(json.load(open\
    # (SCRIPTDIR + "/tools/test/videogame-fantasy_july_reblogfalse_dupe.json", 'r')))

    parse_json_response(response, update)

    if update.errors_title is not None:
        if update.meta_status == '404' and update.meta_msg == 'Not Found':
            print(BColors.FAIL + "Blog {0} is apparently DEAD.".format(blog.name) + BColors.ENDC)
            blog.health = "DEAD"
            update.valid = True
            return update

        if "error" in update.errors and "Unauthorized" in update.errors:
            print(BColors.FAIL + "Blog {0} is unauthorized! Missing API key? REROLL!".format(blog.name) + BColors.ENDC)
            # FIXME: that's assuming only the API key is responsible for unauthorized, might be the IP!
            blog.renew_api_key()
            update.valid = False
            return update

        print(BColors.FAIL + "Blog {0} uncaught error in response: ".format(blog.name, str(update)) + BColors.ENDC)
        update.valid = False
        return update

    blog.health = "UP"

    if update.total_posts < 20:   #FIXME: arbitrary value
        print(BColors.LIGHTYELLOW + "Warning: {0} seems to have been wiped.".format(blog.name) + BColors.ENDC)
        blog.health = "WIPED"

    print(BColors.BLUEOK + "No error in checking json response for {0}".format(blog.name) + BColors.ENDC)

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
        self.last_checked = None
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


    def attach_proxy(self, proxy_object):
        """ attach proxy object, refresh session on update too"""
        if not self.proxy_object: #first time
            self.proxy_object = proxy_object
        else: #we're updating
            self.proxy_object = proxy_object
            self.init_session() # refresh


    def attach_random_api_key(self):
        """ attach api key fetched from global list to proxy object already attached"""

        self.api_key_object_ref = api_keys.get_random_api_key(instances.api_keys)
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
    @count_api_requests
    def requester(self, url, requests_session=None, api_key=None):
        """ Does a request, returns json """
        # url = 'https://httpbin.org/get'

        if not requests_session:
            self.init_session()
        print("---\nRequested new session for {0}: {1} {2}\n----"\
        .format(self.name, self.requests_session.proxies, self.requests_session.headers))

        try:
            response = self.requests_session.get(url, timeout=10)
            # if response.status_code == 200:
            if response.status_code > 0:
                json_data = response.json()
                return json_data
            return response.json() #FIXME: TESTING
        except Exception as e:
            raise


    def api_get_blog_json_health(self, lock, api_key=None):
        """Returns requests.response object"""
        if not api_key:
            api_key = self.api_key_object_ref
        attempt = 0
        apiv2_url = 'https://api.tumblr.com/v2/blog/{0}/info?api_key={1}'.format(self.name, api_key.api_key)
        print(BColors.YELLOW + "api_get_blog_json_health({0}): proxy: {1}".format(self.name, self.proxy_object.get('ip_address')) + BColors.ENDC)
        # renew proxy n times if it fails
        while attempt < 3:
            try:
                response = self.requester(apiv2_url, self.requests_session, api_key)
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




class UpdatePayload(requests.Response):
    """ Container dictionary holding values from json to pass along """
    def __init__(self):
        self.errors_title = None
        self.valid = False

def parse_json_response(json, update):
    """returns a UpdatePayload() object that holds the fields to update in DB"""
    t0 = time.time()

    # if not 200 <= json['meta']['status'] <= 399:
    #     update.errors = json['errors']
    #     return update
    update.meta_status = json.get('meta')['status']
    update.meta_msg = json.get('meta')['msg']

    if not json.get('response') and json.get('errors'):
        update.errors_title = json.get('errors')[0]['title']
        return update

    json = json.get('response')
    update.blogname = json['blog']['name']
    update.total_posts = json['blog']['total_posts']
    update.updated = json['blog']['updated']
    if json['posts']:
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
                pass
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



def get_with_proxies():
    """get API json with proxies"""

def get_without_proxies():
    """get API json with own IP, throttled to not get banned, unique API key"""



if __name__ == "__main__":
    main()
    # try:
    #     main()
    # except Exception as e:
    #     logging.debug("Error in main():" + str(e))
