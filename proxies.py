
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
from constants import BColors, sleep_here

try:
    ua = UserAgent() # init database, retrieves UAs
except errors.FakeUserAgentError as e:
    print(str(e))
    pass

SCRIPTDIR = os.path.dirname(__file__)
fakeresponse = {\
  "args": {},\
  "headers": {\
    "Accept": "*/*",\
    "Accept-Encoding": "gzip, deflate",\
    "Connection": "close",\
    "Host": "httpbin.org",\
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.90 Safari/537.36"\
  },\
  "origin": "108.61.166.245",\
  "url": "https://httpbin.org/get"\
}

class ProxyScanner():
    """Gets proxies, associates UA"""
    def __init__(self):
        self.ua = ""
        self.http_proxies_set = set()
        self.proxy_ua_dict = { "proxies" : [] }
        self.print_lock = threading.Lock()
        self.definitive_proxy_list = None
        self.definitive_proxy_cycle = None
        self.http_proxies_recovered = dict()


    def get_new_proxy(self, old_proxy=None): #TODO: move this to the wallet, to pop out the bad proxy
        print("Removing proxy {0} and getting new one.".format(old_proxy))
        if old_proxy is not None:
            self.definitive_proxy_list.remove(old_proxy)
        if not len(self.definitive_proxy_list): # if list of proxy object is depleted
            self.gen_list_of_proxy_objects(self.get_proxies) # no more api keys here, later on tumblrblog object
        self.gen_proxy_cycle()
        return next(self.definitive_proxy_cycle)


    def gen_proxy_cycle(self):
        self.definitive_proxy_cycle = cycle(self.definitive_proxy_list)
        return self.definitive_proxy_cycle


    def get_random(self, mylist):
        """returns a random item from list"""
        return random.choice(mylist)


    def gen_list_of_proxy_objects(self, fresh_proxy_dict):
        """Returns list of proxy objects, without api_keys fields populated"""
        newlist = list()
        for ip, ua in fresh_proxy_dict.items():
            newlist.append(Proxy(ip, ua))

        self.definitive_proxy_list = newlist

        return self.definitive_proxy_list #FIXME: remove?


    def get_proxies_from_json_on_disk(self, myfilepath=None):
        if not myfilepath: #FIXME: default path for testing
            myfilepath = SCRIPTDIR + os.sep + 'proxies.json'
        data = json.load(open(myfilepath, 'r'))
        self.http_proxies_recovered = data.get('proxies')
        return data.get('proxies')


    def write_proxies_to_json_on_disk(self, data, myfilepath=None):
        if not myfilepath: #FIXME: default path for testing
            myfilepath = SCRIPTDIR + os.sep + 'proxies_saved.json'
        data = json.dump(data, myfilepath)


    def get_proxies(self):
        """Returns a dict of validated IP:UA
        returns None if fetching free list failed"""
        # socks_proxies_list = get_free_socks_proxies("https://socks-proxy.net/", type=socks)
        attempt = 0
        while not self.proxy_ua_dict.get('proxies'):
            if attempt > 5:
                break

            self.http_proxies_set = self.get_free_http_proxies(https_strict=False)

            # we got nothing!
            if len(self.http_proxies_set) == 0:
                attempt += 1
                continue

            useragents_cycle = cycle(self.get_ua_set(len(self.http_proxies_set)))

            # populate with our previously recorded proxies
            self.get_proxies_from_json_on_disk()
            for proxy in self.http_proxies_recovered:
                if proxy.get('disabled'):
                    print(BColors.YELLOW + "From disk, skipping {0} because disabled.".format(proxy.get('ip_address')) + BColors.ENDC)
                    continue
                # self.proxy_ua_dict[proxy.get('ip_address')] = proxy.get('user_agent')
                self.proxy_ua_dict.get('proxies').append(proxy)

            # populate with our fresh set of proxies
            for ip in self.http_proxies_set:
                print(BColors.LIGHTGREEN + "Checking proxy for dupe in set:" + ip + BColors.ENDC)
                for proxy in self.http_proxies_recovered: # filter out those we already have recorded
                    if ip in proxy.get('ip_address'):
                        print(BColors.CYAN + "Skipping {0} because already have it in set.".format(ip) + BColors.ENDC)
                        # self.http_proxies_set.remove(ip)
                        break
                else: # no break has occured
                    temp_dict = { "ip_address": ip, "user_agent": next(useragents_cycle) }
                    self.proxy_ua_dict.get('proxies').append(temp_dict)


            # test our pool of proxies and delete invalid ones from dict
            http_proxy_pool = cycle(self.proxy_ua_dict.get('proxies'))

            def threader():
                while True:
                    # gets an worker from the queue
                    worker = q.get()

                    # Run the example job with the avail worker in queue (thread)
                    self.job_test_proxy(worker, next(http_proxy_pool))

                    # completed with the job
                    q.task_done()

            q = Queue()
            for x in range(10): #FIXME: harrcoded 10 threads
                t = threading.Thread(target=threader)

                # classifying as a daemon, so they will die when the main dies
                t.daemon = True

                # begins, must come after daemon definition
                t.start()
            
            for proxy in self.http_proxies_set:
                q.put(proxy)

            # wait until the thread terminates.
            q.join()

            attempt += 1
        else: #if no break occured
            return self.proxy_ua_dict

        if len(self.http_proxies_set) == 0:
            print(BColors.FAIL + "WARNING: NO PROXIES FETCHED!" + BColors.ENDC)
            return None


    def get_ua_set(self, maxlength):
        """Returns a set of random UAs, of size maxlength"""
        ua_set = set()
        for i in range(0, maxlength): #FIXME: harcoded
            ua_string = ua.random
            ua_set.add(ua_string)
        print("ua_set length:", len(ua_set))
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

        print("Number of proxies fetched:", len(proxies))
        return proxies


    def job_test_proxy(self, worker, proxy_ua_dict):
        # url = 'https://httpbin.org/get'
        url = 'http://www.proxy-checker.org/'
        proxy_ip = proxy_ua_dict.get('ip_address')
        proxy_ua = proxy_ua_dict.get('user_agent')
        headers = {'User-Agent': proxy_ua_dict.get('user_agent')}
        sleep_here()
        pass
        try:
            with self.print_lock:
                print("Testing proxy {0} / UA {1}:".format(proxy_ip, proxy_ua))
            response = requests.get(url, proxies={"http": proxy_ip, "https": proxy_ip}, headers=headers, timeout=11)

            if response.status_code == 200:
                json_data = response.json()
                with self.print_lock:
                    print(BColors.BLUEOK + BColors.GREEN + " origin: " + json_data['origin'] + "/ UA: " + json_data['headers']['User-Agent'] + BColors.ENDC )
            # print(response.json())
        except Exception as e:
            # del self.proxy_ua_dict[proxy]
            with self.print_lock:
                print(BColors.MAGENTA + "removing {0}{2} from {1}".format(proxy_ua_dict, self.proxy_ua_dict, BColors.ENDC))
                print(BColors.FAIL + "Skipping. Connnection error:" + str(e) + BColors.ENDC + "\n")
                self.proxy_ua_dict.get('proxies').remove(proxy_ua_dict)
        # with self.print_lock:
        #     print(threading.current_thread().name,worker)



def write_proxies_to_json():
    # TODO: write proxies by ping quality, disabled, etc.
    pass



class Proxy:
    """holds values to pass to proxy"""

    def __init__(self, ip=None, ua=None, api_key=None, secret_key=None):
        self.ip_address = ip
        self.user_agent = ua
        self.api_key = api_key
        self.secret_key = secret_key


if __name__ == "__main__":
    scanner = ProxyScanner()
    proxies = scanner.get_proxies()
    print("Proxies final: " + str(proxies))
    json.dumps(proxies, indent=True)
