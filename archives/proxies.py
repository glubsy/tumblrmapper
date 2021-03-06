#!/bin/env python3
from lxml.html import fromstring
import requests
# import json
from itertools import cycle
import traceback
from fake_useragent import UserAgent, errors
from constants import BColors 

try:
    ua = UserAgent() # init database, retrieves UAs
except errors.FakeUserAgentError as e:
    print(str(e))
    pass

class ProxyScanner():
    """Gets proxies, associates UA"""
    def __init__(self):
        self.ua = ""
        self.http_proxies_set = set()
        self.proxy_ua_dict = {}
    
    def get_proxies(self):
        """Returns a dict of validated IP:UA
        returns None if fetching free list failed"""
        # socks_proxies_list = get_free_socks_proxies("https://socks-proxy.net/", type=socks)
        i = 0
        while not self.proxy_ua_dict:
            if i > 10:
                break

            self.http_proxies_set = self.get_free_http_proxies(https_strict=False)

            if len(self.http_proxies_set) == 0:
                i += 1
                continue

            useragents_cycle = cycle(self.get_ua_set(len(self.http_proxies_set)))

            for proxy in self.http_proxies_set:
                self.proxy_ua_dict[proxy] = next(useragents_cycle)

            # test our pool of proxies and delete invalid ones from dict
            http_proxy_pool = cycle(self.http_proxies_set)
            self.test_proxies(http_proxy_pool)
            
            i += 1
        else: #executed if no break occured
            return self.proxy_ua_dict
        # executed since break occured
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

        for i in parser.xpath('//tbody/tr')[:80]: #FIXME: hardcoded 80 results per top page
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

    def test_proxies(self, http_proxy_pool):
        url = 'https://httpbin.org/get'
        for i in range(0,len(self.http_proxies_set)):
            #Get a proxy from the pool
            proxy = next(http_proxy_pool)
            headers = {'User-Agent': self.proxy_ua_dict[proxy]}
            print(BColors.BOLD + "Testing proxies: " + str(i) + "/" + str(len(self.http_proxies_set)) + BColors.ENDC)
            try:
                print("proxy / UA:", proxy, "/", self.proxy_ua_dict[proxy], " :")
                response = requests.get(url,proxies={"http": proxy, "https": proxy}, headers=headers, timeout=15)
                # json_data = json.loads(response.text)
                if response.status_code == 200:
                    json_data = response.json()
                    print(BColors.BLUEOK + BColors.OKGREEN + " origin: " + json_data['origin'] + "/ UA: " + json_data['headers']['User-Agent'] + BColors.ENDC )
                # print(response.json())
            except Exception as e:
                del self.proxy_ua_dict[proxy]
                print(BColors.FAIL + "Skipping. Connnection error:" + str(e) + BColors.ENDC + "\n")
        return self.proxy_ua_dict


if __name__ == "__main__":
    scanner = ProxyScanner()
    proxies = scanner.get_proxies()
    print("Final proxies:", proxies)


