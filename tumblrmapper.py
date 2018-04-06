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
import json
import csv
from itertools import cycle
from constants import BColors
import proxies, db_handler 
import tumblr_client
import tumdlr_classes
import tumblr_client
import db_handler
from concurrent import futures
import instances

from rate_limiter import RateLimiter
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
                        "api_keys": "api_keys.txt",
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

    config = parse_config(args.config_path, args.data_path)

    THREADS = config.getint('tumblrmapper', 'threads')

    if args.create_blank_db: # we asked for a brand new DB file
        blogs_toscrape = config.get('tumblrmapper', 'blogs_to_scrape')
        archives_toload = config.get('tumblrmapper', 'archives')
        temp_database_path = config.get('tumblrmapper', 'blank_db')
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
    instances.api_keys = get_api_key_object_list(SCRIPTDIR + os.sep + config.get('tumblrmapper', 'api_keys'))

    # === PROXIES ===
    # Get proxies from free proxies site
    instances.proxy_scanner = proxies.ProxyScanner()
    # fresh_proxy_dict = proxy_scanner.get_proxies()
    # print(fresh_proxy_dict)

    fresh_proxy_dict = {'128.199.198.79:8118': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', '142.0.72.77:808': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36', '80.211.4.187:8080': 'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 1.1.4322)', '188.166.68.38:3128': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', '196.220.96.39:3128': 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; Media Center PC 6.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C)', '176.53.2.122:8080': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36', '147.75.113.108:8080': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.137 Safari/4E423F', '163.172.175.210:3128': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-us) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27', '5.9.107.34:3128': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36', '115.249.145.202:80': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1467.0 Safari/537.36', '36.74.18.249:8080': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36', '41.190.33.162:8080': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.2 Safari/537.36', '80.48.119.28:8080': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36', '94.177.247.162:8118': 'Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36', '89.236.17.108:3128': 'Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20130401 Firefox/21.0', '196.220.96.34:3128': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.517 Safari/537.36', '5.189.133.231:80': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1309.0 Safari/537.17', '173.212.202.65:443': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36', '59.106.215.9:3128': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36', '35.185.39.27:6969': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1664.3 Safari/537.36', '217.194.255.217:3128': 'Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36', '119.11.240.152:3128': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36'}
    
    # Associate api_key to each proxy in fresh list
    instances.proxy_scanner.gen_list_of_proxies_with_api_keys(fresh_proxy_dict, instances.api_keys)
    instances.proxy_scanner.gen_proxy_cycle()

    # === DATABASE ===
    db = db_handler.Database(db_filepath=config.get('tumblrmapper', 'db_filepath'), \
                            username=config.get('tumblrmapper', 'username'),
                            password=config.get('tumblrmapper', 'password'))
    db.connect()

    # === BLOG ===
    blog_object_queue = queue.Queue(maxsize=THREADS)
    
    # while True:
    # == Fetch 10 available blogs from DB ==
    daemon_threads = []
    worker_threads = []

    for i in range(0,THREADS):
        args = (db,)
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
    db.close()


def process(db):
    blog = blog_generator(db)

    if blog.crawl_status == 'new': # not yet updated
        blog.api_get_blog_json_health()
        update_blog_health_db(blog)
        startcrawling(blog)
    elif blog.crawl_status == 'resume': # refresh health info
        api_get_blog_json_health(blog)
        update_blog_health_db(blog)
        offset = read_offset_from_DB(blog)
        # startcrawling(blog, offset)
        print("startcrawling at offset")


    if blog.health == 'UP' or blog.health == 'WIPED':
        startcrawling(blog)
    elif blog.health == DEAD:
        print("dead blog")
        return

def startcrawling(blog):
    print("startcralwing")
    pass
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

def blog_generator(db):
    """ Returns a blog """
    blog = fetch_random_blog(db)
    # attach a proxy 
    blog.proxy_object = next(instances.proxy_scanner.definitive_proxy_cycle)
    # print("current blog's proxy object: " + blog.proxy_object)
    blog.init_session()
    return blog

def worker_get_blog_info(blog):
    """ get info from API """

    blog.api_get_blog_json_health()

def update_blog_health_db(blog, response):
    """ Last checks before updating BLOG table with info"""
    
    print(BColors.GREEN + "Updating DB with:", str(response) + BColors.ENDC)
    # TESTING: 
    # update = tumblr_client.parse_json_response(json.load(open\
    # (SCRIPTDIR + "/tools/test/videogame-fantasy_july_reblogfalse_dupe.json", 'r')))
    
    update = tumblr_client.parse_json_response(json.load(response))
    print("UPDATE:" + update)


    if "error" in update and "404" in update:
        print("Error in json response!")
        update.health = "DEAD"
        db_handler.update_blog_info(blog, update)
        return
    if "error" in update and "Unauthorized" in update.errors['title']:
        print("Unauthorized! Missing API key?")
        #FIXME: reroll API KEY?
        return 

    update.health = "UP"

    #FIXME: arbitrary value
    if update.total_posts < 20: 
        update.health = "WIPED"

    print(BColors.BLUEOK + "no problem, updating" + BColors.ENDC)
    db_handler.update_blog_info(blog, update)

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

        print("---\nRequested new session for {0}: {1} {2}\n----".format(self.name, requests_session.proxies, requests_session.headers))
        if not requests_session:
            self.init_session()

        try:
            response = self.requests_session.get(url, timeout=10)
            # if response.status_code == 200:
            if response.status_code > 0:
                json_data = response.json()
                return json_data
            return response.json() #FIXME: TESTING 
        except Exception as e:
            raise
        return None


    def api_get_blog_json_health(self):
        attempt = 0
        apiv2_url = 'https://api.tumblr.com/v2/blog/{0}/info?api_key={1}'.format(self.name, self.proxy_object.api_key)
        print("api_get_blog_json_health({0})".format(apiv2_url))
        # renew proxy n times if it fails
        while attempt < 3:
            try: 
                response = self.requester(apiv2_url, self.requests_session)
            except requests.exceptions.ProxyError as e:
                self.proxy_object = instances.proxy_scanner.get_new_proxy(self.proxy_object)
                print(BColors.FAIL + "ProxyError on {0}! Changing proxy in get health to: {1}"\
                .format(self.name, self.proxy_object.ip_address) + BColors.ENDC)
                attempt += 1
                continue
            except requests.exceptions.Timeout as e:
                self.proxy_object = instances.proxy_scanner.get_new_proxy(self.proxy_object)
                print(BColors.FAIL + "Timeout on {0}! Changing proxy in get health to: {1}"\
                .format(self.name, self.proxy_object.ip_address) + BColors.ENDC )
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
