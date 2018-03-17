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



myproxies = [
    127.0.0.1,
    127.0.0.1
]
# requests.get(url, proxies=myproxies)

class Initscrape():
    """main class"""
    def __init__(self):
        """take care of initializing vars"""
        self.config = Config.parse_config()



class Scraper(object):
    """"""
    def __init__(self, object):
        self.



    def get_config




class Config(object):
    """stores configuration options"""

    def __init__(self):
        pass

    def parse_config():
        """parse config from config file"""

        argparser = argparse.ArgumentParser(description=\
        "crawls tumblr APIs for url and store them in a Firebird DB")

        group = argparser.add_mutually_exclusive_group()

        args = argparser.parse_args()

        MAIN_OBJ.input_dirorlist = args.input
        MAIN_OBJ.outputdir = str(args.outputdir).rstrip("/") + os.sep
        MAIN_OBJ.filelist = args.filelist + os.sep + constants.FILELIST
        MAIN_OBJ.errorlist = str(args.errorlist).rstrip("/") + os.sep + constants.ERRORLIST
        MAIN_OBJ.original_filelist = constants.TMP + os.sep + constants.ORIGINAL_FILELIST
        MAIN_OBJ.original_filelistselected = constants.TMP + os.sep + constants.ORIGINAL_FILELIST_SELECTED
        MAIN_OBJ.maxseconds = args.maxseconds
        MAIN_OBJ.dbfile = args.source_check #TODO: add a separate arg for separate path holding security2.fdb
        MAIN_OBJ.db_list = str(args.db_list).rstrip("/") + os.sep + constants.DB_CHECKED_LIST
        MAIN_OBJ.dl_loglist = str(args.dl_loglist).rstrip("/") + os.sep + constants.DOWNLOAD_LIST


if __name__ == "__main__":
    try:
        Initscrape.main()