import csv
import json
import time
import os
import random
import threading
# import tumblrmapper
from constants import BColors
import instances


def count_api_requests(func):
    def func_wrapper(*args, **kwargs):
        api_key = kwargs.get("api_key")
        api_key.use_once()
        print(BColors.LIGHTPINK + "API key used: {0}. Number of request left: {1}/{2}"\
        .format(api_key.api_key, api_key.bucket_hour, api_key.bucket_day) + BColors.ENDC)
        return func(*args, **kwargs)
    return func_wrapper



def get_api_key_object_list(api_keys_filepath):
    """Returns a list of APIKey objects"""
    api_key_list = list()

    # for key, secret in read_api_keys_from_csv(api_keys_filepath + '.txt').items():
    #     api_key_list.append(APIKey(key, secret))
    for item in read_api_keys_from_json(api_keys_filepath):
        api_key_list.append(APIKey(\
                            api_key=item.get('api_key'),\
                            secret_key=item.get('secret_key'),\
                            hour_check_time=item.get('hour_check_time'),\
                            day_check_time=item.get('day_check_time'),\
                            last_used_time=item.get('last_used_time'),\
                            disabled=item.get('disabled'),\
                            disabled_until=item.get('disabled_until'),\
                            blacklisted=item.get('blacklisted')\
                            ))

    return api_key_list


def read_api_keys_from_json(myfilepath=None):
    """ Returns a list of dictionaries """
    if not myfilepath:
        myfilepath = instances.config.get('tumblrmapper', 'api_keys')

    data = json.load(open(myfilepath, 'r'))
    return data.get('api_keys')


def write_api_keys_to_json(data=None, myfilepath=None):
    """ Saves all api keys attributes to disk for later use"""
    if not data:
        data = instances.api_keys # list
    if not myfilepath:
        myfilepath = instances.config.get('tumblrmapper', 'api_keys')

    api_dict = { "api_keys": []}
    for item in data:
        api_dict['api_keys'].append(item.__dict__)
    json.dumps(api_dict, indent=True)
    with open(myfilepath, 'w') as f:
        json.dump(api_dict, f, indent=True) 


def disable_api_key(api_key_object_list):
    """ this API key caused problem, might have been flagged, add to temporary blacklist"""

    print(BColors.RED + "disabling API key {0} from instances.api_keys list".format(api_key_object_list) + BColors.ENDC)

    # key = item for item in instances.api_keys if item == api_key_object_list
    key = next((i for i in instances.api_keys if i == api_key_object_list), None)

    if not key:
        print(BColors.RED + "Did not find this key in instances.api_keys list!?" + BColors.ENDC)
        return

    if key.disabled:
        print("key {0} is already disabled!".format(key.api_key))
    else:
        key.disabled = True

    for key in instances.api_keys:
        print(BColors.RED + "API key {0} is disabled: {1}".format(key.api_key, key.disabled) + BColors.ENDC)

    write_api_keys_to_json()


def get_random_api_key(apikey_list=None):
    """ get a random not disabled api key from instances.api_keys list"""
    
    if not apikey_list:
        apikey_list = instances.api_keys
    
    attempt = 0
    while True:
        keycheck = random.choice(apikey_list)
        # print(keycheck.disabled, keycheck.api_key)
        attempt += 1
        if attempt >= len(apikey_list):
            break
        if not keycheck.disabled:
            return keycheck
    print(BColors.FAIL + BColors.BLINKING + 'Attempts exhausted api_key list length! All keys are disabled! Renew them!' + BColors.ENDC)
    #TODO: handle this critical error later (exit gracefully)


def remove_key(api_key_object):
    """ Completely remove API key object instance from the pool [not used for now]"""
    # FIXME: when length reaches 0, error will occur!
    try:
        instances.api_keys.remove(api_key_object)
    except Exception as e:
        print(BColors.FAIL + str(e) + BColors.ENDC)
        pass





class APIKey:
    """Api key object to keep track of requests per hour, day"""
    request_max_hour = 1000
    request_max_day = 5000
    epoch_day = 86400
    epoch_hour = 3600

    def __init__(self, *args, **kwargs):
        self.api_key = kwargs.get('api_key', None)
        self.secret_key = kwargs.get('secret_key', None)
        self.request_num = 0
        self.hour_check_time = kwargs.get('hour_check_time', float())
        self.day_check_time = kwargs.get('day_check_time', float())
        self.last_used_hour = kwargs.get('last_used_hour', float())
        self.last_used_day = kwargs.get('last_used_day', float())
        self.disabled = kwargs.get('disabled', None)
        self.disabled_until = kwargs.get('disabled_until', None)
        self.blacklisted = kwargs.get('blacklisted', False)
        self.bucket_hour = float(1000)
        self.bucket_day = float(5000)

    def disable_until(self, duration):
        """returns date until it's disabled"""
        now = time.time()
        self.disabled_until = now + duration

    def is_disabled(self):
        if self.disabled or self.blacklisted:
            return True
        return False

    def use_once(self):
        self.bucket_hour -= 1
        self.bucket_day -= 1

        if self.bucket_hour <= 0:
            self.disable_until(1800) # half an hour
        if self.bucket_day <= 0:
            self.disable_until(44200) # 12 hours


def threaded_buckets():
    """ Infinite loop, that adds a token per second to each API object's buckets"""

    t = threading.Thread(target=bucket_inc)
    t.daemon
    t.start()

    def bucket_inc():
        while True:
            time.sleep(5)
            for api_obj in instances.api_keys:
                if api_obj.bucket_hour >= 1000:
                    continue
                api_obj.bucket_hour += 1.39
            for api_obj in instances.api_keys:
                if api_obj.bucket_day >= 5000:
                    continue
                api_obj.bucket_hour += 0.29


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
    SCRIPTDIR = os.path.dirname(__file__)
    API_KEYS_FILE = SCRIPTDIR + os.sep + "api_keys.json"
    args = tumblrmapper.parse_args()
    instances.config = tumblrmapper.parse_config(args.config_path, args.data_path)

    instances.api_keys = get_api_key_object_list(API_KEYS_FILE)

    # for item in instances.api_keys:
    #     disable_api_key(item)
    # get_random_api_key(instances.api_keys)

