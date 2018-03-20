#!/bin/env python3
import os
import sys
import argparse
import signal
import time
import random
import requests
import threading
import queue
#import json
from constants import BColors
import proxies

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

myproxies = (
    127.0.0.1,
    127.0.0.1
)
# requests.get(url, proxies=myproxies)

class Init():
    """main class"""

    def __init__(self):
        """take care of initializing vars"""
        self.config = Config.parse_config()

    def get_config(self):
        #STUB
        pass

class TumblrRequest():
    """"""
    def __init__(self):
        """Init attributes holding info about scraped target""" 
        self.blogname = ""
        self.post_offset = ""



class Config():
    """stores configuration options"""

    def __init__(self):
        pass

    def parse_config(self):
        """parse config from config file"""

        argparser = argparse.ArgumentParser(description=\
        "crawls tumblr APIs for url and store them in a Firebird DB")

        group = argparser.add_mutually_exclusive_group()

        args = argparser.parse_args()



class Get_with_Proxies():
    """get API json with proxies"""


class Get_with_own_IP():
    """get API json with own IP, throttled to not get banned, unique API key"""



if __name__ == "__main__":
    try:
        Initscrape.main()



