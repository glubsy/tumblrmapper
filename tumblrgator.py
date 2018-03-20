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

# try:
#     from tqdm import tqdm
#     TQDM_AVAILABLE = True
# except ImportError:
#     TQDM_AVAILABLE = False

class Init():
    """main class"""

    def __init__(self):
        """take care of initializing vars"""
        logging.basicConfig(format='%(levelname)s:%(message)s',
                            level=getattr(logging, args.log_level.upper()))
        logging.debug("Debugging Enabled.")
        self.config = Config.parse_config(self)

    def main(self):
        """main loop"""




class TumblrRequest():
    """"""
    def __init__(self):
        """Init attributes holding info about scraped target""" 
        self.blogname = ""
        self.post_offset = ""


class Config:
    """stores configuration options"""

    def __init__(self):
        self.parse_config()

    def parse_config(self):
        """Configuration derived from defaults & file."""
        script_dir = os.path.realpath(__file__) + os.sep
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
                            "api": "v2", #use api v2 by default
                            "proxies": "no" #use random proxies, or not
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


class Blog(db_handler.ConnectedDB):
    """blog object, holding retrieved values to pass along"""

    def __init__(self):
        self.name = None
        self.totalpostcount = 0
        self.post_offset = 0


class Proxy(Blog):
    """holds values to pass to proxy"""

    def __init__(self):
        self.ipaddress = None
        self.apikey = None

class Post(Blog):
    """holds values to post content"""

    def __init__(self):
        self.tumblr_id = None

class ImageURL(): #FIXME: descendent of Blog or Post?
    """holds values to images linked in a post"""

    def __init__(self):
        self.address = None




def Get_with_Proxies():
    """get API json with proxies"""


def Get_with_own_IP():
    """get API json with own IP, throttled to not get banned, unique API key"""



if __name__ == "__main__":
    try:
        Init.main()
    except Exception as e:
        logging.debug("Error in Init:" + str(e))
