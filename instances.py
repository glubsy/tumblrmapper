import random
import time

def __init__():
    """ stores global variables """
    random.seed()
    global proxy_scanner
    proxy_scanner = None
    global api_keys
    api_keys = None
    global config
    config = None

def sleep_here(minwait=None, maxwait=None):
    if not minwait and not maxwait:  
        time.sleep(random.randrange(1, 5))
    else:
        time.sleep(random.randrange(minwait, maxwait))