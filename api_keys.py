import csv
import json
import time
import os
import random
import threading
import logging
import traceback
from itertools import cycle
# import tumblrmapper
from constants import BColors
import instances
try:
    from requests_oauthlib import OAuth1
    OAUTHLIB_AVAIL = True
except ImportError:
    OAUTHLIB_AVAIL = False

# logging = logging.getLogger()


class APIKeyDepleted(Exception):
    """We assume we have checked that all API keys are now disabled"""
    def __init__(self, message=None):
        if not message:
            message = "No more API key available"
        super().__init__(message)
        self.next_date_avail = self.get_closest_date()

    def get_closest_date(self):
        """Read all api key fields for the closest date in which an API is available again"""
        date_set = set()
        for key in instances.api_keys:
            if key.disabled or key.blacklisted: # not really necessary
                date_set.add(max(key.disabled_until, key.blacklisted_until))
        return min(date_set)


class ListCycle(list):
    """Keeps an up to date cycle of its items. Returns None if list is empty and
    next() has been called on it."""

    __slots__ = ['_iter', '_cycle', 'cur', ]

    def __init__(self, _iter=None):
        if not _iter:
            _iter = []
        super().__init__(_iter)
        self._cycle = None
        self.cur = 0

    def __next__(self):
        if self._cycle is None:
            self.gen_cycle(init=True)
        try:
            return next(self._cycle)
        except StopIteration:
            return

    def gen_cycle(self, init=False):
        self._cycle = self.cycle(self, init=init)

    def append(self, item):
        super().append(item)
        self.gen_cycle()

    def remove(self, item):
        index = self.index(item)
        if index >= self.cur:
            self.cur -= 1 # we'll miss one

        super().remove(item)
        self.gen_cycle()

    def cycle(self, iterable, init=False):
        if init:
            self.cur = -1
        try:
            while True:
                self.cur += 1

                if not len(iterable):
                    return

                if self.cur >= len(iterable):
                    self.cur = 0

                yield iterable[self.cur]
        except GeneratorExit:
            #https://amir.rachum.com/blog/2017/03/03/generator-cleanup/
            pass
        except BaseException as e:
            traceback.print_exc()
            logging.debug(f"{BColors.RED}Exception in cycle of ListCycle() \
cursor={self.cur} length={len(iterable)}: {e}{BColors.ENDC}")


def get_api_key_object_list(api_keys_filepath):
    """Returns a list cycle of APIKey objects"""
    api_key_list_cycle = ListCycle()

    # for key, secret in read_api_keys_from_csv(api_keys_filepath + '.txt').items():
    #     api_key_list.append(APIKey(key, secret))
    for item in read_api_keys_from_json(api_keys_filepath):
        api_key_list_cycle.append(APIKey(
                            api_key=item.get('api_key'),
                            secret_key=item.get('secret_key'),
                            oauth_token=item.get('oauth_token'),
                            oauth_secret=item.get('oauth_secret'),
                            hour_check_time=item.get('hour_check_time'),
                            day_check_time=item.get('day_check_time'),
                            last_written_time=item.get('last_written_time'),
                            last_used_hour=item.get('last_used_hour'),
                            last_used_day=item.get('last_used_day'),
                            bucket_hour=item.get('bucket_hour'),
                            bucket_day=item.get('bucket_day'),
                            disabled=item.get('disabled'),
                            disabled_until=item.get('disabled_until', 0),
                            disabled_until_h=item.get('disabled_until_h'),
                            blacklisted=item.get('blacklisted'),
                            blacklist_hit=0,
                            blacklisted_until=item.get('blacklisted_until', 0),
                            blacklisted_until_h=item.get('blacklisted_until_h')
                            ))

    return api_key_list_cycle


def read_api_keys_from_json(myfilepath=None):
    """ Returns a list of dictionaries """
    if not myfilepath:
        myfilepath = instances.config.get('tumblrmapper', 'api_keys')

    data = json.load(open(myfilepath, 'r'))
    return data.get('api_keys')


def write_api_keys_to_json(keylist=None, myfilepath=None):
    """ Saves all api keys attributes to disk"""
    if not keylist:
        keylist = instances.api_keys # list
    if not myfilepath:
        myfilepath = instances.config.get('tumblrmapper', 'api_keys')

    api_dict = { "api_keys": [] }
    for obj in keylist:
        obj.last_written_time = int(time.time())
        api_dict['api_keys'].append(obj.__dict__)

    # Remove field holding a non serializable object
    for dict_repr in api_dict['api_keys']:
        if dict_repr.get('oauth') is not None:
            del dict_repr['oauth']

    # DEBUG
    # print(BColors.MAGENTA + "write_api_keys_to_json: {0}"\
    # .format(json.dumps(api_dict, indent=True)) + BColors.ENDC)

    if len(api_dict.get('api_keys')) == 0:
        logging.error(f"{BColors.FAIL}Error when building API key list of dicts: \
length is 0! Skipping writing to disk!{BColors.ENDC}")
        return

    with open(myfilepath, 'w') as f:
        json.dump(api_dict, f, indent=True)


def disable_api_key(api_key_object, blacklist=False, duration=3600):
    """This API key caused problem, might have been flagged, add to temporary blacklist
    for a default of one hour"""

    logging.info(f"{BColors.RED}Disabling API key {api_key_object.api_key} \
from instances.api_keys list{BColors.ENDC}")

    # key = item for item in instances.api_keys if item == api_key_list_object
    key = next((i for i in instances.api_keys if i == api_key_object), None)

    if not key: # should never happen
        logging.error(f"{BColors.RED}Did not find this key in instances.api_keys list!?{BColors.ENDC}")
        return

    if blacklist:
        key.blacklist_until(duration=duration)

    if key.disabled:
        logging.debug(f"{BColors.BOLD}Key {key.api_key} is already disabled \
until {time.ctime(key.disabled_until)}!{BColors.ENDC}")
    else:
        key.disable_until(duration=duration)

    for key in instances.api_keys:
        logging.debug(f"{BColors.RED}API key {key.api_key} is disabled: \
{key.disabled}. blacklisted: {key.blacklisted}.{BColors.ENDC}")

    write_api_keys_to_json()


def get_random_api_key(apikey_list=None):
    """ get a random not disabled api key from instances.api_keys list"""

    # write current state in case we crash and lose status info
    write_api_keys_to_json()

    if not apikey_list:
        apikey_list = instances.api_keys

    attempt = 0
    while attempt < len(apikey_list):
        keycheck = next(apikey_list, apikey_list[attempt])
        attempt += 1
        # if attempt >= len(apikey_list):
        #     break
        if keycheck.disabled:
            if keycheck.check_enable():
                return keycheck
        else:
            return keycheck
#     logging.critical(f'{BColors.FAIL}{BColors.BLINKING}Attempts exhausted api_key \
# list length! All keys are disabled! Renew them!{BColors.ENDC}')
    raise APIKeyDepleted



def remove_key(api_key_object):
    """Deprecated. Completely remove API key object instance from the pool [not used for now]"""
    # FIXME: when length reaches 0, error will occur!
    try:
        instances.api_keys.remove(api_key_object)
    except Exception as e:
        logging.error(f'{BColors.FAIL}Error trying to remove API key {e}{BColors.ENDC}')
        pass


class APIKey:
    """Api key object to keep track of requests per hour, day"""
    # request_max_hour = 1000
    # request_max_day = 5000
    # epoch_day = 86400
    # epoch_hour = 3600
    # cannot use __slots__ here because we need its dict to write to disk
    def __init__(self, *args, **kwargs):
        self.api_key = kwargs.get('api_key')
        self.secret_key = kwargs.get('secret_key')
        self.oauth_token = kwargs.get('oauth_token')
        self.oauth_secret = kwargs.get('oauth_secret')
        self.hour_check_time = kwargs.get('hour_check_time', int())
        self.day_check_time = kwargs.get('day_check_time', int())
        self.last_used_hour = kwargs.get('last_used_hour', int(time.time()))
        self.last_used_day = kwargs.get('last_used_day', int(time.time()))
        self.last_written_time = kwargs.get('last_written_time', int())
        self.disabled = kwargs.get('disabled')
        self.disabled_until = kwargs.get('disabled_until', 0)
        self.disabled_until_h = kwargs.get('disabled_until_h')
        self.blacklisted = kwargs.get('blacklisted', False)
        self.blacklist_hit = kwargs.get('blacklist_hit', 0)
        self.blacklisted_until = kwargs.get('blacklisted_until', 0)
        self.blacklisted_until_h = kwargs.get('blacklisted_until_h')
        self.bucket_hour = kwargs.get('bucket_hour', float(1000))
        self.bucket_day = kwargs.get('bucket_day', float(5000))
        self.print_count = 0
        self.oauth = self.init_oauth()

    def init_oauth(self):
        if OAUTHLIB_AVAIL:
            return OAuth1(
            self.api_key,
            client_secret=self.secret_key,
            resource_owner_key=self.oauth_token,
            resource_owner_secret=self.oauth_secret
            )
        logging.debug(f"requests_oauth was not found, no oauth field available.")
        return None

    def disable_until(self, duration=3600):
        """duration in second, computes date from now until it's disabled"""
        now = int(time.time())
        self.disabled = True
        self.disabled_until = now + duration
        self.disabled_until_h = time.ctime(self.disabled_until)
        logging.warning(f"{BColors.MAGENTA}API key {self.api_key} just got \
disabled until {self.disabled_until_h}{BColors.ENDC}")


    def blacklist_until(self, duration=3600):
        """duration in second, computes date from now until it's disabled"""
        now = int(time.time())
        self.blacklisted = True
        self.blacklisted_until = now + duration
        self.blacklisted_until_h = time.ctime(self.blacklisted_until)
        logging.warning(f"{BColors.RED}API key {self.api_key} blacklisted until \
{self.blacklisted_until_h}{BColors.ENDC}")


    def check_enable(self):
        """If not disabled or blacklisted, returns True,
        if disabled_until is overdue, enable and returns True, else returns False"""

        if not self.disabled and not self.blacklisted:
            return True

        now = int(time.time())
        if (now >= self.disabled_until) and (now >= self.blacklisted_until):
            self.disabled = False
            self.blacklisted = False
            self.disabled_until = 0
            self.disabled_until_h = None
            self.blacklisted_until = 0
            self.blacklisted_until_h = None
            return True
        return False


    def enable(self):
        """Just force enable"""
        if self.disabled:
            self.disabled = False
            self.disabled_until = 0
            self.disable_until_h = None


    def is_disabled(self, check_type=None):
        """Check if the key use tokens are depleted and disable if necessary"""
        if check_type == "hour" or check_type == "both":
            if self.bucket_hour <= 0:
                self.disable_until(3600) # an hour #FIXME can be rolled in sooner

        if check_type == "day" or check_type == "both":
            if self.bucket_day <= 0:
                self.disable_until(86400) # 24 hours #FIXME can be rolled in sooner

        if self.disabled or self.blacklisted:
            return True
        return False


    def use(self, use_count=1):
        """Decrements bucket of token by one, disable if reaches 0"""
        now = int(time.time())

        self.bucket_hour -= use_count
        if (now - self.last_used_hour) > 3600: #LU was > 1 hour, update checkpoint
            self.last_used_hour = now
        self.is_disabled("hour")

        self.bucket_day -= use_count
        if (now - self.last_used_day) > 86400: #LU was > 1 day, update checkpoint
            self.last_used_day = now
        self.is_disabled("day")

        self.inc_print_usage()


    def refund(self, amount=1):
        """Increments back buckets of tokens by amount"""
        self.bucket_hour += amount
        self.bucket_day += amount

        if self.disabled:
            if self.bucket_hour > 1 and self.bucket_day > 1:
                self.enable()


    def inc_print_usage(self):
        """Increment the use count and print status every 10 uses"""
        self.print_count += 1
        if (self.print_count % 10) == 0: # every 10 times, display it
            logging.warning(f"{BColors.MAGENTA} API key used: {self.api_key}. \
Number of request left: {self.bucket_hour}/{self.bucket_day}{BColors.ENDC}")



def threaded_buckets():
    """ Infinite loop, adds a token every 5 second to each API object's buckets"""

    # Computes counters back to what they've incremented to while were were offline
    now = int(time.time())
    for api_obj in instances.api_keys:
        diff = now - api_obj.last_written_time
        if not (now - api_obj.last_used_hour) < 3600: # less than an hour
            api_obj.bucket_hour = min(api_obj.bucket_hour + ((diff/5) * 1.390), 1000) # clamped

    #         logging.debug(BColors.MAGENTA + "Compute API status. Key {0}: \nbucket hour {1}, \
    # now {2} last_written_time {3}, difference {4}, diff/5 {5}, *1.390={6}"
    #         .format(api_obj.api_key, api_obj.bucket_hour,
    #         now, api_obj.last_written_time, diff, (diff/5), ((diff/5) * 1.390)) + BColors.ENDC)

        if not (now - api_obj.last_used_day) < 86400: # less than a day
            # 86400 / 5000 = 0.05787037
            api_obj.bucket_day = min(api_obj.bucket_day +  ((diff/5) * 0.2892), 5000)

    #         logging.debug(BColors.MAGENTA + "Compute API status. Key {0}: \nbucket day {1}, \
    # now {2} last_written_time {3}, difference {4}, diff/5 {5}, *0.2892={6}"
    #         .format(api_obj.api_key, api_obj.bucket_day,
    #         now, api_obj.last_written_time, diff, (diff/5), ((diff/5) * 0.2892)) + BColors.ENDC)

        logging.warning(BColors.MAGENTA + "Request left for API key {0}: hour {1} day {2}"
        .format(api_obj.api_key, api_obj.bucket_hour, api_obj.bucket_day) + BColors.ENDC)

    t = threading.Thread(target=bucket_inc)
    t.daemon = True
    t.start()


def bucket_inc():
    while True:
        time.sleep(5)
        now = int(time.time())
        for api_obj in instances.api_keys:
            if api_obj.bucket_hour > 1000:
                api_obj.bucket_hour = 1000
                continue
            else:
                if (now - api_obj.last_used_hour) <= 3600: # too early
                    continue
                api_obj.bucket_hour += 1.390

        for api_obj in instances.api_keys:
            if api_obj.bucket_day > 5000:
                api_obj.bucket_day = 5000
                continue
            else:
                if (now - api_obj.last_used_day) <= 86400: # too early
                    continue
                api_obj.bucket_day += 0.2892


def read_api_keys_from_csv(myfilepath):
    """Deprecated.
    Returns dictionary of multiple {api_key: secret}"""
    kvdict = dict()
    with open(myfilepath, 'r') as f:
        mycsv = csv.reader(f)
        for row in mycsv:
            key, secret = row
            kvdict[key] = secret
    return kvdict


if __name__ == "__main__":
    import tumblrmapper
    SCRIPTDIR = os.path.dirname(__file__)
    API_KEYS_FILE = SCRIPTDIR + os.sep + "api_keys.json"
    args = tumblrmapper.parse_args()
    instances.config = tumblrmapper.parse_config(args.config_path, args.data_path)

    instances.api_keys = get_api_key_object_list(API_KEYS_FILE)

    # for item in instances.api_keys:
    #     disable_api_key(item)
    # get_random_api_key(instances.api_keys)

