#!/bin/env python3
import os
import sys
import argparse
import signal
import time
import logging
import configparser
import requests
import threading
import queue
#import json
import csv
from itertools import cycle
from constants import BColors
import proxies, db_handler 
import tumblr_client
import tumdlr_classes
import tumblr_client
import db_handler
from concurrent import futures

from rate_limiter import RateLimiter
# try:
#     from tqdm import tqdm
#     TQDM_AVAILABLE = True
# except ImportError:
#     TQDM_AVAILABLE = False
SCRIPTDIR = os.path.dirname(__file__) + os.sep
THREADS = 10
proxy_scanner = None

def parse_args():
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description='Tumblrgator tumblr url mapper.')
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


def parse_config(config_path, data_path):
    """Configuration derived from defaults & file."""

    config_path = SCRIPTDIR + "config" #FIXME: hardcoded configpath
    data_path = SCRIPTDIR  #FIXME: hardcoded datapath

    # Set some config defaults
    config_defaults = { "config_path": config_path,
                        "data_path": data_path,
                        "blogs_to_scrape": data_path + "blogs_to_scrape.txt", #initial blog list to populate DB
                        "scraper_log": data_path + "scraper.log", #log for downloads and proxies
                        "blank_db": data_path + "blank_db.fdb", #blank initial DB file
                        "api_version": "2", #use api v2 by default
                        "proxies": False, #use random proxies, or not
                        "api_keys": "api_keys.txt",
                        "threads": 10
                        }

    config = configparser.SafeConfigParser(config_defaults)
    config.add_section('tumblrgator')

    # Try to read config file (either passed in, or default value)
    # conf_file = os.path.join(config.get('tumblrgator', 'config_path'), 'config')
    logging.debug("Trying to read config file: %s", config_path)
    result = config.read(config_path)
    if not result:
        logging.debug("Unable to read config file: %s", config_path)

    config.set('tumblrgator', 'blogs_to_scrape', SCRIPTDIR + config.get('tumblrgator', 'blogs_to_scrape'))
    config.set('tumblrgator', 'archives', SCRIPTDIR + config.get('tumblrgator', 'archives'))
    config.set('tumblrgator', 'db_filepath', os.path.expanduser(config.get('tumblrgator', 'db_filepath')))
    
    logging.debug("Merged config: %s",
                sorted(dict(config.items('tumblrgator')).items()))

    return config



def main():
    """Entry point"""
    # parse command-line arguments
    args = parse_args()

    logging.basicConfig(format='%(levelname)s:%(message)s',
                        level=getattr(logging, args.log_level.upper()))
    logging.debug("Debugging Enabled.")

    config = parse_config(args.config_path, args.data_path)

    THREADS = config.getint('tumblrgator', 'threads')

    if args.create_blank_db: # we asked for a brand new DB file
        blogs_toscrape = SCRIPTDIR + "tools/blogs_toscrape.txt"
        archives_toload = SCRIPTDIR +  "tools/1280_files_list.txt"
        temp_database_path = config.get('tumblrgator', 'blank_db')
        temp_database = db_handler.Database(filepath=temp_database_path, \
                                username="sysdba", password="masterkey")
        
        db_handler.create_blank_database(temp_database)
        db_handler.populate_db_with_blogs(temp_database, blogs_toscrape)
        # Optional archives too
        # db_handler.populate_db_with_archives(temp_database, archives_toload)
        print(BColors.BLUEOK + BColors.OKGREEN + "Done creating blank DB in: " + temp_database.db_filepath + BColors.ENDC)
        return

    # === API KEY ===
    # list of APIKey objects
    api_keys = get_api_key_object_list(SCRIPTDIR + config.get('tumblrgator', 'api_keys'))

    # === PROXIES ===
    # Get proxies from free proxies site
    global proxy_scanner
    proxy_scanner = proxies.ProxyScanner()
    # fresh_proxy_dict = proxy_scanner.get_proxies()
    # print(fresh_proxy_dict)

    # fresh_proxy_dict = {'92.53.73.138:8118': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36', '122.183.139.104:8080': 'Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', '122.183.243.68:8080': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', '119.42.87.147:3128': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36', '103.74.245.12:65301': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36', '143.202.208.133:53281': 'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36', '192.116.142.153:8080': 'Mozilla/5.0 (Windows NT 6.1;WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17', '80.211.4.187:8080': 'Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36', '59.106.215.9:3128': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/29.0', '5.35.2.235:3128': 'Opera/9.80 (X11; Linux i686; U; ja) Presto/2.7.62 Version/11.01', '122.183.137.190:8080': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130401 Firefox/21.0', '42.104.84.106:8080':'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', '128.68.87.239:8080': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36', '41.190.33.162:8080': 'Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', '212.237.34.18:8888': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36'}
    fresh_proxy_dict = {'5.53.73.138:8118': '10_10_1) AppleWebKit/537.36 (KHTML, like Ge', '92.53.73.138:8118': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 S'}

    # Associate api_key to each proxy in fresh list
    proxy_scanner.gen_list_of_proxies_with_api_keys(fresh_proxy_dict, api_keys)
    proxy_scanner.gen_proxy_cycle()

    # === DATABASE ===
    db = db_handler.Database(db_filepath=config.get('tumblrgator', 'db_filepath'), \
                            username=config.get('tumblrgator', 'username'),
                            password=config.get('tumblrgator', 'password'))
    con = db.connect()

    # === BLOG ===
    blog_object_queue = queue.Queue()
    blog_object_list = list()
    # == Fetch 10 available blogs from DB ==
    for i in range(0, THREADS):
        blog = fetch_random_blog(db)
        # attach a proxy 
        blog.proxy_object = next(proxy_scanner.definitive_proxy_cycle)
        # print("current blog's proxy object: " + blog.proxy_object)
        
        # # put in a queue
        # blog_object_queue.put(blog)
        # # fetch from queue one
        # current_blog = blog_object_queue.get()
        # spawn one TEST Process 


        blog.init_session()
        blog_object_list.append(blog)

    with futures.ThreadPoolExecutor(THREADS) as executor:
        info_jobs = [executor.submit(blog.api_get_blog_health) for blog in blog_object_list]
        futures.wait(info_jobs)
        result = info_jobs[0].result()
        process_job = executor.submit(update_blog_health_db, result)
        # process_job.add_done_callback()


    # blog_testing_queue.put(TumblrBlog('videogame-fantasy'))
    # definitive_proxy_list[0]
    # blog_object_queue.put(TumblrBlog()) #FIXME: hardcoded index for testing


    # === Start async procedures here === 

    # # When prerequisite is completed
    # with futures.ThreadPoolExecutor(THREADS) as executor:
    #     jobs = [executor.submit(get_posts, blog) for blog in blogs] #"blog" is an iterable of single arguments
    #     if keyboard_interrupt:
    #         for job in jobs:
    #             jobs.cancel()
    #     for comp_job in futures.as_completed(jobs):
    #         response = comp_job.result()
    #         executor.submit(insert_into_db, response)



    # fetch first blog in DB that is unchecked
    # - fetch info from API about health
    # - populate total_posts, validate health, and crawling status
    # - on result from procedure, pass the resulting TumblrBlog object to next function
    # add to queue
    # start fetching tumblr API from offset
    # populate DB, update Blog crawling status, posts scraped so far
    # display progress (total_posts / done_posts)
    # handle keyboard interrupt

def update_blog_health_db(response):
    print("updating DB with:", str(response))
    return


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
        self.requests_session = None
    

    def init_session(self, requests_session=None): #FIXME: 
        if not self.requests_session:
            requests_session = requests.Session()
            requests_session.headers.update({'User-Agent': self.proxy_object.user_agent})
            requests_session.proxies.update({'http': self.proxy_object.ip_address, 'https': self.proxy_object.ip_address})
        self.requests_session = requests_session


    def requester(self, url, requests_session=None):
        """ Does a request, returns json """
        # url = 'https://httpbin.org/get'

        print("Requesting with a session: " + str(requests_session.proxies) + str(requests_session.headers) + "\n")
        if not requests_session:
            self.init_session()

        try:
            response = self.requests_session.get(url, timeout=10)
            # if response.status_code == 200:
            if response.status_code > 0:
                json_data = response.json()
                return json_data
        except Exception as e:
            raise


        return None


    def api_get_blog_health(self):
        attempt = 0
        apiv2_url = 'https://api.tumblr.com/v2/blog/{0}/info'.format(self.name)
        print("getting api health: " + apiv2_url)
        # renew proxy 4 times if it fails
        global proxy_scanner
        while attempt < 1:
            try: 
                response = self.requester(apiv2_url, self.requests_session)
            except requests.exceptions.ProxyError as e:
                self.proxy_object = proxy_scanner.get_new_proxy(self.proxy_object)
                print("ProxyError! Changing proxy in get health to:", self.proxy_object.ip_address)
                attempt += 1
                continue
            except requests.exceptions.Timeout as e:
                self.proxy_object = proxy_scanner.get_new_proxy(self.proxy_object)
                print("Timeout! Changing proxy in get health to:", self.proxy_object.ip_address)
                attempt += 1
                continue
            return response
        return None






#FIXME: delete?
class ProxyGenerator():
    """Yields the next proxy in the list"""
    def init(self, proxylist):
        self._complete_proxy_list = proxylist

    def __iter__(self):
        """returns a Proxy() object from a stale list"""
        while self.complete_proxy_list:
            yield self._complete_proxy_list.pop()


def get_api_key_object_list(api_keys_filepath):
    """Returns a list of APIKey objects"""
    api_key_list = list()

    for key, secret in read_api_keys_from_csv(api_keys_filepath).items():
        api_key_list.append(APIKey(key, secret))

    return api_key_list


def read_api_keys_from_csv(myfilepath):
    """Returns dictionary of multiple {api_key: secret}"""
    kvdict = dict()
    with open(myfilepath, 'r') as f:
        mycsv = csv.reader(f)
        for row in mycsv:
            key, secret = row
            kvdict[key] = secret
    return kvdict


class APIKey:
    """Api key object to keep track of requests per hour, day"""
    request_max_hour = 1000
    request_max_day = 5000
    epoch_day = 86400
    epoch_hour = 3600

    def __init__(self, api_key=None, secret_key=None):
        self.api_key = api_key
        self.secret_key = secret_key
        self.request_num = 0
        self.first_used = float()
        self.last_used = float()
        self.disabled = False
        self.disabled_until = float()

    def disable_until(self):
        """returns date until it's disabled"""
        now = time.time()
        pass #TODO: stub

    def is_valid(self):
        now = time.time()
        if self.request_num >= request_max_day:
            return False

        if self.request_num >= request_max_hour:
            return False
        return True
        #TODO: stub

    def use_once(self):
        now = time.time()
        if self.first_used == 0.0:
            self.first_used = now
        self.request_num += 1
        self.last_used = now
    #TODO: stub


def fetch_random_blog(database):
    """Queries DB for a blog that is either new or needs update. 
    Returns a TumblrBlog() object instance with no proxy attached to it."""
    blog = TumblrBlog()
    blog.name, blog.total_posts, blog.health, \
    blog.crawl_status, blog.post_scraped, \
    blog.offset, blog.last_checked = db_handler.fetch_random_blog(database)
    return blog




class Proxy:
    """holds values to pass to proxy"""

    def __init__(self, ip=None, ua=None, api_key=None, secret_key=None):
        self.ip_address = ip
        self.user_agent = ua
        self.api_key = api_key
        self.secret_key = secret_key



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
