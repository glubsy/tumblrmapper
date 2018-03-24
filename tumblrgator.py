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
from constants import BColors
import proxies, db_handler
import tumblr_client
import tumbdlr_classes
import tumblr_client
import db_handler
# try:
#     from tqdm import tqdm
#     TQDM_AVAILABLE = True
# except ImportError:
#     TQDM_AVAILABLE = False

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

    script_dir = os.path.dirname(__file__) + os.sep
    config_path = script_dir + "config" #FIXME: hardcoded configpath
    data_path = script_dir  #FIXME: hardcoded datapath

    # Set some config defaults
    config_defaults = { "config_path": config_path,
                        "data_path": data_path,
                        "queue_toscrape": data_path + "queue_toscrape", #initial blog list to populate DB
                        "queue_scraping": data_path + "queue_scraping", #currently processing blogs (for resuming)
                        "queue_donescraped": data_path + "queue_donescraped", #blogs done scraped entirely ?
                        "scraper_log": data_path + "scraper.log", #log for downloads and proxies
                        "blank_db": data_path + "blank_db.fdb", #blank initial DB file
                        "api_version": "2", #use api v2 by default
                        "proxies": False #use random proxies, or not
                        } 

    config = configparser.SafeConfigParser(config_defaults)
    config.add_section('tumblrgator')

    # Try to read config file (either passed in, or default value)
    # conf_file = os.path.join(config.get('tumblrgator', 'config_path'), 'config')
    logging.debug("Trying to read config file: %s", config_path)
    result = config.read(config_path)
    if not result:
        logging.debug("Unable to read config file: %s", config_path)

    logging.debug("Merged config: %s",
                sorted(dict(config.items('tumblrgator')).items()))

    return config


def main():
    """main loop"""
    # parse command-line arguments
    args = parse_args()

    logging.basicConfig(format='%(levelname)s:%(message)s',
                        level=getattr(logging, args.log_level.upper()))
    logging.debug("Debugging Enabled.")

    config = parse_config(args.config_path, args.data_path)

    #start looping through blogs scrapers
    TumblrBlog(config).




class TumblrBlog:
    """blog object, holding retrieved values to pass along"""

    def __init__(self):
        self.name = None
        self.totalpostcount = 0
        self.post_offset = 0

class Proxy:
    """holds values to pass to proxy"""

    def __init__(self):
        self.ipaddress = None
        self.apikey = None

class Process:
    """Processus fetching a Blog and writing to a DB with a Connection, with an optional Proxy"""


def get_with_proxies():
    """get API json with proxies"""

def simple_get():
    """get API json with own IP, throttled to not get banned, unique API key"""



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.debug("Error in Init:" + str(e))
