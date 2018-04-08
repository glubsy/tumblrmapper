import csv
import json
import os
import random
from constants import BColors
import instances


def get_api_key_object_list(api_keys_filepath):
    """Returns a list of APIKey objects"""
    api_key_list = list()

    # for key, secret in read_api_keys_from_csv(api_keys_filepath + '.txt').items():
    #     api_key_list.append(APIKey(key, secret))
    for item in read_api_keys_from_json(api_keys_filepath + '.json'):
        api_key_list.append(APIKey(\
                            api_key=item.get('key'),\
                            secret_key=item.get('secret'),\
                            hour_check_time=item.get('hour_check_time'),\
                            day_check_time=item.get('day_check_time'),\
                            last_used_time=item.get('last_used_time'),\
                            disabled=item.get('disabled'),\
                            disabled_until=item.get('disabled_until'),\
                            blacklisted=item.get('blacklisted')\
                            ))

    return api_key_list


def read_api_keys_from_json(myfilepath):
    """ Returns a list of dictionaries """

    data = json.load(open(myfilepath, 'r')) 
    return data.get('api_keys')
    

def dump_api_keys_to_json(myfilepath):
    """ Saves all api keys attributes to disk for later use"""
    # TODO: STUB
    # json.dump()
    pass


def disable_api_key(api_key_object):
    """ this API key caused problem, might have been flagged, add to temporary blacklist"""

    print(BColors.RED + "disabling API key {0} from instances.api_keys list".format(api_key_object) + BColors.ENDC)

    # key = item for item in instances.api_keys if item == api_key_object
    key = next((i for i in instances.api_keys if i == api_key_object), None)

    if not key:
        print(BColors.RED + "Did not find this key in instances.api_keys list!?" + BColors.ENDC)
        return

    key.disabled = True

    for key in instances.api_keys:
        print(BColors.RED + "API key {0} is disabled: {1}".format(key.api_key, key.disabled) + BColors.ENDC)


def get_random_api_key(apikey_list):
    """ get a random not disabled api key from instances.api_keys list"""
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


def remove_key(api_key_object):
    """ Completely remove API key object instance from the pool [not used for now]"""
    # FIXME: when length reaches 0, error will occur!
    try:
        instances.api_key.remove(api_key_object)
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
        self.last_used_time = kwargs.get('last_used_time', float())
        self.disabled = kwargs.get('disabled', None)
        self.disabled_until = kwargs.get('disabled_until', None)
        self.blacklisted = kwargs.get('blacklisted', False)

    def disable_until(self):
        """returns date until it's disabled"""
        now = time.time()
        pass #TODO: stub

    def is_valid(self):
        now = time.time()
        if self.request_num >= request_max_day:
            return False

        if self.request_num >= request_max_hour:
            return False
        return True
        #TODO: stub

    def use_once(self):
        now = time.time()
        if self.first_used == 0.0:
            self.first_used = now
        self.request_num += 1
        self.last_used = now
    #TODO: stub



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
    API_KEYS_FILE = SCRIPTDIR + os.sep + "api_keys_proxies"

    instances.api_keys = get_api_key_object_list(API_KEYS_FILE)

    for item in instances.api_keys:
        disable_api_key(item)
    get_random_api_key(instances.api_keys)

