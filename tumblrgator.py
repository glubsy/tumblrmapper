#!/bin/env python3
import os
import sys
import argparse
import signal
import time
import random
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

from rate_limiter import RateLimiter
# try:
#     from tqdm import tqdm
#     TQDM_AVAILABLE = True
# except ImportError:
#     TQDM_AVAILABLE = False
SCRIPTDIR = os.path.dirname(__file__) + os.sep
THREADS = 10

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
    scanner = proxies.ProxyScanner()
    # fresh_proxy_dict = scanner.get_proxies()
    fresh_proxy_dict = {'96.220.96.35': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36',
                        '104.236.175.94': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17',
                        '89.236.17.108': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36' }

    # Associate api_key to each proxy in fresh list
    definitive_proxy_list = gen_list_of_proxies_with_api_keys(fresh_proxy_dict, api_keys)
    definitive_proxy_cycle = cycle(definitive_proxy_list)
    # print(definitive_proxy_list)

    # === DATABASE ===
    db = db_handler.Database(db_filepath=config.get('tumblrgator', 'db_filepath'), \
                            username=config.get('tumblrgator', 'username'),
                            password=config.get('tumblrgator', 'password'))
    con = db.connect()

    # === BLOG ===
    blog_object_queue = queue.Queue()


    for i in range(0, THREADS):
        blog = fetch_random_blog(db)
        blog.proxy_object = next(definitive_proxy_cycle)
        blog_object_queue.put(blog)

        # ping_test_blog(blog_object_queue[i])


    
    # blog_testing_queue.put(TumblrBlog('videogame-fantasy'))
    # definitive_proxy_list[0]
    # blog_object_queue.put(TumblrBlog()) #FIXME: hardcoded index for testing


    # === Start async procedures here === 




    # fetch first blog in DB that is unchecked
    # - fetch info from API about health
    # - populate total_posts, validate health, and crawling status
    # - on result from procedure, pass the resulting TumblrBlog object to next function
    # add to queue
    # start fetching tumblr API from offset
    # populate DB, update Blog crawling status, posts scraped so far
    # display progress (total_posts / done_posts)
    # handle keyboard interrupt




def Requester(url, proxy):
    """ Does a request """


def gen_list_of_proxies_with_api_keys(fresh_proxy_dict, api_keys):
    """Returns list of proxy objects, with their api key and secret key populated"""
    newlist = list()
    for ip, ua in fresh_proxy_dict.items():
        random_apik = get_random(api_keys)
        key, secret = random_apik.api_key, random_apik.secret_key
        newlist.append(Proxy(ip, ua, key, secret))
    return newlist

def get_random(mylist):
    """returns a random item from list"""
    return random.choice(mylist)


class ProxyGenerator(): #FIXME: delete?
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


class Proxy:
    """holds values to pass to proxy"""

    def __init__(self, ip=None, ua=None, api_key=None, secret_key=None):
        self.ip_address = ip
        self.user_agent = ua
        self.api_key = api_key
        self.secret_key = secret_key

class Process:
    """Processus fetching a Blog and writing to a DB with a Connection, with an optional Proxy"""


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
