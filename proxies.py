
import json
import os
#!/bin/env python3
# import json
import random
import threading
import time
from collections import namedtuple
import traceback
from itertools import cycle
from queue import Queue

import logging
import requests
from fake_useragent import UserAgent, errors
from lxml.html import fromstring
import instances
# import tumblrmapper
from constants import BColors

try:
    ua = UserAgent() # init database, retrieves UAs
except errors.FakeUserAgentError as e:
    logging.exception(e)
    pass

SCRIPTDIR = os.path.dirname(__file__)

class ProxyScanner():
    """Gets proxies, associates UA"""

    def __init__(self, proxies_path=None):
        if not proxies_path:
            proxies_path = SCRIPTDIR + os.sep + 'proxies.json' #for testing only

        self.proxies_path = proxies_path
        self.http_proxies_set = set() #temp set of proxies from free site
        self.proxy_ua_dict = { "proxies" : [] } # the big global dict
        self.print_lock = threading.Lock()
        self.proxy_ua_dict_lock = threading.Lock()
        self.definitive_proxy_cycle = None #cycle of the list of dicts above
        self.http_proxies_recovered = set() #dicts of proxies previously recorded on disk
        self.restore_proxies_from_disk(proxies_path)


    def restore_proxies_from_disk(self, proxies_path=None):
        """populate with our previously recorded proxies"""
        if not proxies_path:
            proxies_path = self.proxies_path

        self.http_proxies_recovered = filter_dictionary_for_unique(self.get_proxies_from_json_on_disk(proxies_path))
        for proxy in self.http_proxies_recovered:
            if proxy.get('disabled', False) or proxy.get('blacklisted', False):
                logging.debug(BColors.YELLOW + "From disk, skipping {0} because blacklisted.".format(proxy.get('ip_address')) + BColors.ENDC)
                continue
            # self.proxy_ua_dict[proxy.get('ip_address')] = proxy.get('user_agent')
            self.proxy_ua_dict.get('proxies').append(proxy)

        logging.debug(BColors.GREEN + "Restored proxies from disk: {0}".format(len(self.proxy_ua_dict.get('proxies'))) + BColors.ENDC)


    def get_new_proxy(self, old_proxy=None, remove=None):
        """ Returns a new proxy from the regenerated definitive_proxy_cycle
        if remove="remove", remove proxy that is unresponsive from the list and regen cycle
        if remove="blacklist", save proxy in json as blacklisted to never use it ever again"""

        logging.debug("Removing proxy {0} and getting new one.".format(old_proxy))

        if remove == "remove" and old_proxy is not None:
            self.proxy_ua_dict.get('proxies').remove(old_proxy)

        elif remove == "blacklist" is not None: # no remove,
            dict_index = self.proxy_ua_dict.get('proxies').index(old_proxy) #WARNING: assuming the dict is this exact value!
            self.proxy_ua_dict.get('proxies')[dict_index].update({"blacklisted": True}) # add the property
            self.write_proxies_to_json_on_disk(self.proxy_ua_dict)

        if not len(self.proxy_ua_dict.get('proxies')): # if list of proxy dict is depleted
            logging.info(BColors.LIGHTGREEN + BColors.BOLD + "List of proxies is empty, getting from internet!" + BColors.ENDC)
            self.get_proxies_from_internet()
            self.gen_proxy_cycle() #FIXME: move into get_proxies()?

        return next(self.definitive_proxy_cycle)


    def gen_proxy_cycle(self, solid_dict=None):
        """ Regens the definitive_proxy_cycle"""
        if not solid_dict:
            solid_dict = self.proxy_ua_dict.get('proxies')

        self.definitive_proxy_cycle = cycle(solid_dict)


    def get_random(self, mylist):
        """returns a random item from list"""
        return random.choice(mylist)


    def get_proxies_from_json_on_disk(self, myfilepath=None):
        """ Returns the list of proxy dictionaries found in proxies json"""

        if not myfilepath: #FIXME: default path for testing
            myfilepath = SCRIPTDIR + os.sep + 'proxies.json'

        try:
            data = json.load(open(myfilepath, 'r'))
        except json.decoder.JSONDecodeError: # got malformed or empty json
            logging.debug(BColors.FAIL + "Failed decoding proxies json" + BColors.ENDC)
            return []
        # self.http_proxies_recovered = data.get('proxies')

        return data.get('proxies')


    def write_proxies_to_json_on_disk(self, data=None, myfilepath=None):

        if not myfilepath: #FIXME: default path for testing
            myfilepath = SCRIPTDIR + os.sep + 'proxies.json'
        if not data:
            data = self.proxy_ua_dict

        newlist = data.get('proxies') + self.http_proxies_recovered

        data['proxies'] = filter_dictionary_for_unique(newlist) # merging with saved blacklisted proxies

        # DEBUG
        # logging.debug("New dict: {0}".format(data.get('proxies')))

        with open(myfilepath, "w") as f:
            json.dump(data, f, indent=True)



    def get_proxies_from_internet(self):
        """Returns a dict of validated IP:UA
        returns None if fetching free list failed"""
        big_dict = self.with_threads()
        self.write_proxies_to_json_on_disk(big_dict)
        return big_dict


    def with_threads(self):

        # socks_proxies_list = get_free_socks_proxies("https://socks-proxy.net/", type=socks)
        attempt = 0
        while not self.proxy_ua_dict.get('proxies'):
            if attempt > 5:
                break

            self.http_proxies_set = self.get_free_http_proxies(https_strict=False)

            # we got nothing!
            if len(self.http_proxies_set) == 0:
                attempt += 1
                continue #re-run!

            useragents_cycle = cycle(self.get_ua_set(len(self.http_proxies_set)))

            self.restore_proxies_from_disk()

            # populate with our fresh set of proxies
            for ip in self.http_proxies_set:
                # logging.debug(BColors.BOLD + "Checking {0} for dupe in recovered set: ".format(ip) + BColors.ENDC)
                for proxy in self.http_proxies_recovered: # filter out those we already have recorded
                    if ip in proxy.get('ip_address'):
                        logging.debug(BColors.CYAN + "Skipping {0} because already have it in http_proxies_set.".format(ip) + BColors.ENDC)
                        # self.http_proxies_set.remove(ip)
                        break
                else: # no break has occured
                    temp_dict = { "ip_address": ip, "user_agent": next(useragents_cycle), "disabled" : False }
                    self.proxy_ua_dict.get('proxies').append(temp_dict)

            logging.debug(BColors.LIGHTCYAN + "proxy_ua_dict populated: {0} ".format(self.proxy_ua_dict) + BColors.ENDC)

            def threader():
                while True:
                    # gets an worker from the queue
                    worker = q.get()

                    # Run the example job with the avail worker in queue (thread)
                    self.job_test_proxy(worker)

                    # completed with the job
                    q.task_done()

            q = Queue()
            for x in range(10): #FIXME: harrcoded 10 threads
                t = threading.Thread(target=threader)

                # classifying as a daemon, so they will die when the main dies
                t.daemon = True

                # begins, must come after daemon definition
                t.start()

            for proxy in self.proxy_ua_dict.get('proxies'):
                q.put(proxy)

            # wait until the thread terminates.
            q.join()

            attempt += 1
        else: #if no break occured
            return self.proxy_ua_dict

        if len(self.http_proxies_set) == 0:
            logging.debug(BColors.FAIL + "WARNING: NO PROXIES FETCHED!" + BColors.ENDC)
            return None


    def get_ua_set(self, maxlength):
        """Returns a set of random UAs, of size maxlength"""
        ua_set = set()
        for i in range(0, maxlength): #FIXME: hardcoded
            ua_string = ua.random
            ua_set.add(ua_string)
        logging.debug("ua_set length: {0}".format(len(ua_set)))
        return ua_set


    def get_free_http_proxies(self, url="https://free-proxy-list.net/", https_strict=True, header_strict=2):
        """Returns a set() of up to 10 http or socks proxies
        from free-proxy-list.net or socks-proxy.net
        if https_strict, only retains https capable proxies
        header_strict=[0-2], 0=all types accepted, 1=no Transparent, 2=only Elite Proxies"""
        response = requests.get(url)
        parser = fromstring(response.text)
        proxies = set()
        not_transparent = ''
        if "socks-proxy.net" in url:
            if header_strict >= 1:
                not_transparent += './/td[6][not(contains(text(),"Transparent"))]'
            if header_strict > 1:
                not_transparent += '[not(contains(text(),"Anonymous"))]'
            has_https = './/td[7][contains(text(),"Yes")]'
        else:
            if header_strict >= 1:
                not_transparent = './/td[5][not(contains(text(),"transparent"))]'
            if header_strict > 1:
                not_transparent += '[not(contains(text(),"anonymous"))]'
            has_https = './/td[7][contains(text(),"yes")]'

        for i in parser.xpath('//tbody/tr')[:40]: #FIXME: hardcoded 80 results per top page
            if https_strict: #FIXME: the following should be refactored
                if i.xpath(has_https):
                    if header_strict:
                        if i.xpath(not_transparent): # Grabbing IP and corresponding PORT
                            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                            proxies.add(proxy)
                    else:
                        proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                        proxies.add(proxy)
                else:
                    proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                    proxies.add(proxy)
            else: # we don't care about https
                if header_strict:
                    if i.xpath(not_transparent):
                        proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                        proxies.add(proxy)
                else:
                    proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                    proxies.add(proxy)

        logging.debug("Number of proxies fetched: {0}".format(len(proxies)))
        return proxies


    def job_test_proxy(self, proxy_dict, debug=False):
        if debug:
            url = 'https://httpbin.org/get'
        else:
            url = 'https://www.yahoo.com/'
        proxy_ip = proxy_dict.get('ip_address')
        proxy_ua = proxy_dict.get('user_agent')
        headers = {'User-Agent': proxy_dict.get('user_agent')}
        instances.sleep_here()
        pass
        try:
            with self.print_lock:
                logging.debug("Testing proxy {0} / UA {1}:".format(proxy_ip, proxy_ua))
            response = requests.get(url, proxies={"http": proxy_ip, "https": proxy_ip}, headers=headers, timeout=11)

            if response.status_code == 200 or response.status_code == 404:
                if debug:
                    json_data = response.json()
                    with self.print_lock:
                        logging.debug(BColors.BLUEOK + BColors.GREEN + "Origin: {0} -- UA: {1}"\
                        .format(json_data.get('origin'), json_data.get('headers').get('User-Agent')) + BColors.ENDC )
                else:
                    logging.debug(BColors.GREEN + "Response: {0}".format(response) + BColors.ENDC)
        except (requests.exceptions.ProxyError, requests.exceptions.Timeout,\
            requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.SSLError) as e:
            with self.print_lock:
                logging.debug(BColors.FAIL + "Connnection error for {0}: {1}".format(proxy_ip, e) + BColors.ENDC)
                logging.debug(BColors.MAGENTA + "Exception! Removing {0}{2} from {1}".format(proxy_ip, 'proxy pool and recovered', BColors.ENDC))
            with self.proxy_ua_dict_lock:
                try:
                    self.proxy_ua_dict.get('proxies').remove(proxy_dict)
                    for proxy in self.http_proxies_recovered:
                        if proxy.get('ip_address') == proxy_dict.get('ip_address'):
                            logging.debug(BColors.MAGENTA + "Also removing {0} from recovered list".format(proxy_dict.get('ip_address')) + BColors.ENDC)
                            self.http_proxies_recovered.remove(proxy_dict)
                except Exception as e:
                    logging.debug(BColors.MAGENTA + BColors.BOLD + "Error removing proxy {0} from pool: {1}"\
                    .format(proxy_dict.get('ip_address'), e) + BColors.ENDC)
        # with self.print_lock:
        #     logging.debug(threading.current_thread().name,worker)

def filter_dictionary_for_unique(mylist):
    cache = set()
    for dictionary in mylist:
        if dictionary.get('ip') in cache:
            mylist.remove(dictionary)
        else:
            cache.add(dictionary.get('ip'))
    return mylist


class Proxy:
    """holds values to pass to proxy"""

    def __init__(self, ip=None, ua=None, api_key=None, secret_key=None):
        self.ip_address = ip
        self.user_agent = ua
        self.api_key = api_key
        self.secret_key = secret_key


if __name__ == "__main__":
    scanner = ProxyScanner()
    proxies = scanner.get_proxies_from_internet()
    # print("Proxies final: " + str(proxies))
    json.dumps(proxies, indent=True)