import csv
import json
import time
import os
import random
import threading
import logging
from itertools import cycle
# import tumblrmapper
from constants import BColors
import instances

# logging = logging.getLogger()


class APIKeyDepleted(Exception):
    """We assume we have checked that all API keys are now disabled"""
    def __init__(self, message):
        super().__init__(message=message)
        self.next_date_avail = self.get_closest_date()

    def get_closest_date(self):
        """Read all api key fields for the closest date in which an API is available again"""
        date_set = set()
        for key in instances.api_keys:
            if key.is_disabled: # not really necessary
                date_set.add(max(key.disabled_until, key.blacklisted_until))
        return min(date_set)


class ListCycle(list):
    """Keeps an up to date cycle of its items"""
    # caveat: cycle is reset on append/remove
    # potential fix, reimplement Cycle() ourselves

    def __init__(self):
        super().__init__()
        self._cycle = None

    def __next__(self):
        if self._cycle is None:
            self.gen_cycle()
        return next(self._cycle)

    def gen_cycle(self):
        self._cycle = cycle(i for i in self)

    def append(self, item):
        self.gen_cycle()
        super().append(item)

    def remove(self, item):
        self.gen_cycle()
        super().remove(item)


def get_api_key_object_list(api_keys_filepath):
    """Returns a list cycle of APIKey objects"""
    api_key_list_cycle = ListCycle()

    # for key, secret in read_api_keys_from_csv(api_keys_filepath + '.txt').items():
    #     api_key_list.append(APIKey(key, secret))
    for item in read_api_keys_from_json(api_keys_filepath):
        api_key_list_cycle.append(APIKey(
                            api_key=item.get('api_key'),
                            secret_key=item.get('secret_key'),
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

    # DEBUG
    # print(BColors.MAGENTA + "write_api_keys_to_json: {0}"\
    # .format(json.dumps(api_dict, indent=True)) + BColors.ENDC)

    with open(myfilepath, 'w') as f:
        json.dump(api_dict, f, indent=True)


def disable_api_key(api_key_object, blacklist=False, duration=3600):
    """This API key caused problem, might have been flagged, add to temporary blacklist
    for a default of one hour"""

    logging.info(BColors.RED + "Disabling API key {0} from instances.api_keys list".format(api_key_object) + BColors.ENDC)

    # key = item for item in instances.api_keys if item == api_key_list_object
    key = next((i for i in instances.api_keys if i == api_key_object), None)

    if not key: # should never happen
        logging.error(BColors.RED + "Did not find this key in instances.api_keys list!?" + BColors.ENDC)
        return

    if blacklist:
        key.blacklist_until(duration=duration)

    if key.disabled:
        logging.debug(BColors.BOLD + "Key {0} is already disabled!".format(key.api_key) + BColors.ENDC)
    else:
        key.disable_until(duration=duration)

    for key in instances.api_keys:
        logging.debug(BColors.RED + "API key {0} is disabled: {1}. blacklisted: {2}."
        .format(key.api_key, key.disabled, key.blacklisted) + BColors.ENDC)

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
        if keycheck.is_disabled():
            if keycheck.enable():
                return keycheck
        else:
            return keycheck
    logging.critical(BColors.FAIL + BColors.BLINKING +
    'Attempts exhausted api_key list length! All keys are disabled! Renew them!'
    + BColors.ENDC)
    raise APIKeyDepleted



def remove_key(api_key_object):
    """Deprecated. Completely remove API key object instance from the pool [not used for now]"""
    # FIXME: when length reaches 0, error will occur!
    try:
        instances.api_keys.remove(api_key_object)
    except Exception as e:
        logging.error(f'{BColors.FAIL}Error trying to remove API key {e}{BColors.ENDC}')
        pass


def inc_key_request(api_key):
    api_key.use_once()
    api_key.print_count += 1
    if (api_key.print_count % 10) == 0: # every 10 times, display it
        logging.warning(BColors.MAGENTA + "API key used: {0}. Number of request left: {1}/{2}"\
        .format(api_key.api_key, api_key.bucket_hour, api_key.bucket_day) + BColors.ENDC)


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


    def disable_until(self, duration=3600):
        """duration in second, computes date from now until it's disabled"""
        now = int(time.time())
        self.disabled = True
        self.disabled_until = now + duration
        self.disabled_until_h = time.ctime(self.disabled_until)
        logging.warning("{0}API key {1} disabled until {2}{3}".format(
            BColors.MAGENTA, self.api_key, self.disabled_until_h, BColors.ENDC))


    def blacklist_until(self, duration=3600):
        """duration in second, computes date from now until it's disabled"""
        now = int(time.time())
        self.blacklisted = True
        self.blacklisted_until = now + duration
        self.blacklisted_until_h = time.ctime(self.blacklisted_until)
        logging.warning("{0}API key {1} blacklisted until {2}{3}".format(
            BColors.RED, self.api_key, self.blacklisted_until_h, BColors.ENDC))

    def is_disabled(self):
        if self.disabled or self.blacklisted:
            return True
        return False


    def enable(self):
        """If not disabled or blacklisted, returns True,
        if disabled_until is overdue, enable and returns True, else returns False"""
        now = int(time.time())
        if not self.disabled and not self.blacklisted:
            return True
        if (now >= self.disabled_until) and (now >= self.blacklisted_until):
            self.disabled = False
            self.blacklisted = False
            self.disabled_until = 0
            self.disabled_until_h = None
            self.blacklisted_until = 0
            self.blacklisted_until_h = None
            return True
        return False


    def use_once(self):
        """Decrements bucket of tocken by one, disable if reaches 0"""
        now = int(time.time())

        if (now - self.last_used_hour) < 3600: # not too early
            self.bucket_hour -= 1
            if self.bucket_hour <= 0:
                self.disable_until(3600) # an hour s #FIXME can be rolled in sooner
        else:
            self.last_used_hour = now

        if (now - self.last_used_day) < 86400: # not too early
            self.bucket_day -= 1
            if self.bucket_day <= 0:
                self.disable_until(86400) # 24 hours #FIXME can be rolled in sooner
        else:
            self.last_used_day = now


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

