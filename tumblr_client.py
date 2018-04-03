#!/bin/env python3
from builtins import object
import urllib.parse
import requests
import sys
PY3 = sys.version_info[0] == 3
from requests_oauthlib import OAuth1
from requests.exceptions import TooManyRedirects, HTTPError
from functools import wraps

def validate_params(valid_options, params):
    """
    Helps us validate the parameters for the request

    :param valid_options: a list of strings of valid options for the
                          api request
    :param params: a dict, the key-value store which we really only care about
                   the key which has tells us what the user is using for the
                   API request

    :returns: None or throws an exception if the validation fails
    """
    #crazy little if statement hanging by himself :(
    if not params:
        return

    #We only allow one version of the data parameter to be passed
    data_filter = ['data', 'source', 'external_url', 'embed']
    multiple_data = [key for key in params.keys() if key in data_filter]
    if len(multiple_data) > 1:
        raise Exception("You can't mix and match data parameters")

    #No bad fields which are not in valid options can pass
    disallowed_fields = [key for key in params.keys() if key not in valid_options]
    if disallowed_fields:
        field_strings = ",".join(disallowed_fields)
        raise Exception("{0} are not allowed fields".format(field_strings))

def validate_blogname(fn):
    """
    Decorator to validate the blogname and let you pass in a blogname like:
        client.blog_info('codingjester')
    or
        client.blog_info('codingjester.tumblr.com')
    or
        client.blog_info('blog.johnbunting.me')

    and query all the same blog.
    """
    @wraps(fn)
    def add_dot_tumblr(*args, **kwargs):
        if (len(args) > 1 and ("." not in args[1])):
            args = list(args)
            args[1] += ".tumblr.com"
        return fn(*args, **kwargs)
    return add_dot_tumblr




class TumblrRequest(object):
    """
    A simple request object that lets us query the Tumblr API
    """

    __version = "0.0.7"

    def __init__(self, consumer_key, consumer_secret="", oauth_token="", oauth_secret="", host="https://api.tumblr.com"):
        self.host = host
        self.oauth = OAuth1(
            consumer_key,
            client_secret=consumer_secret,
            resource_owner_key=oauth_token,
            resource_owner_secret=oauth_secret
        )
        self.consumer_key = consumer_key

        self.headers = {
            "User-Agent": "pytumblr/" + self.__version,
        }

    def get(self, url, params):
        """
        Issues a GET request against the API, properly formatting the params

        :param url: a string, the url you are requesting
        :param params: a dict, the key-value of all the paramaters needed
                       in the request
        :returns: a dict parsed of the JSON response
        """
        url = self.host + url
        if params:
            url = url + "?" + urllib.parse.urlencode(params)

        try:
            resp = requests.get(url, allow_redirects=False, headers=self.headers, auth=self.oauth)
        except TooManyRedirects as e:
            resp = e.response

        return self.json_parse(resp)

    def json_parse_validate_reponse(self, response): #FIXME: not used for now, but can be inspiring
        """ Wraps and abstracts response validation and JSON parsing
        to make sure the user gets the correct response.
        :param response: The response returned to us from the request
        :returns: a dict of the json response """
        try:
            data = response.json()
        except ValueError:
            data = {'meta': { 'status': 500, 'msg': 'Server Error'}, 'response': {"error": "Malformed JSON or HTML was returned."}}

        # We only really care about the response if we succeed
        # and the error if we fail
        if 200 <= data['meta']['status'] <= 399:
            return data['response']
        else:
            return data




class TumblrRestClient(object):
    """A Python Client for the Tumblr API"""

    def __init__(self, consumer_key, consumer_secret="", oauth_token="", oauth_secret="", host="https://api.tumblr.com"):
        """Initializes the TumblrRestClient object, creating the TumblrRequest
        object which deals with all request formatting.

        :param consumer_key: a string, the consumer key of your
                             Tumblr Application
        :param consumer_secret: a string, the consumer secret of
                                your Tumblr Application
        :param oauth_token: a string, the user specific token, received
                            from the /access_token endpoint
        :param oauth_secret: a string, the user specific secret, received
                             from the /access_token endpoint
        :param host: the host that are you trying to send information to,
                     defaults to https://api.tumblr.com
        :returns: None """
        self.request = TumblrRequest(consumer_key, consumer_secret, oauth_token, oauth_secret, host)

    @validate_blogname
    def posts(self, blogname, type=None, **kwargs):
        """
        Gets a list of posts from a particular blog

        :param blogname: a string, the blogname you want to look up posts
                         for. eg: codingjester.tumblr.com
        :param id: an int, the id of the post you are looking for on the blog
        :param tag: a string, the tag you are looking for on posts
        :param limit: an int, the number of results you want
        :param offset: an int, the offset of the posts you want to start at.
        :param filter: the post format you want returned: HTML, text or raw.
        :param type: the type of posts you want returned, e.g. video. If omitted returns all post types.

        :returns: a dict created from the JSON response
        """
        if type is None:
            url = '/v2/blog/{0}/posts'.format(blogname)
        else:
            url = '/v2/blog/{0}/posts/{1}'.format(blogname, type)
        return self.send_api_request(url, kwargs, ['id', 'tag', 'limit', 'offset', 'reblog_info', 'notes_info', 'filter', 'api_key'], True)

    @validate_blogname
    def blog_info(self, blogname):
        """
        Gets the information of the given blog

        :param blogname: the name of the blog you want to information
                         on. eg: codingjester.tumblr.com

        :returns: a dict created from the JSON response of information
        """
        url = "/v2/blog/{0}/info".format(blogname)
        return self.send_api_request(url, {}, ['api_key'], True)

    def send_api_request(self, url, params={}, valid_parameters=[], needs_api_key=False):
        """
        Sends the url with parameters to the requested url, validating them
        to make sure that they are what we expect to have passed to us

        :param method: a string, the request method you want to make
        :param params: a dict, the parameters used for the API request
        :param valid_parameters: a list, the list of valid parameters
        :param needs_api_key: a boolean, whether or not your request needs an api key injected

        :returns: a dict parsed from the JSON response
        """
        if needs_api_key:
            params.update({'api_key': self.request.consumer_key})
            valid_parameters.append('api_key')

        validate_params(valid_parameters, params) #FIXME: might not need
        return self.request.get(url, params)


















class UpdatePayload(dict):
    pass

def parse_json_response(json): #TODO: move to client module when done testing
    """returns a UpdatePayload() object that holds the fields to update in DB"""
    t0 = time.time()
    update = UpdatePayload()
    update.blogname = json['blog']['name']
    update.totalposts = json['blog']['total_posts']
    update.posts_response = json['posts'] #list of dicts
    update.trimmed_posts_list = [] #list of dicts of posts

    for post in update.posts_response: #dict in list
        current_post_dict = {}
        current_post_dict['id'] = post.get('id')
        current_post_dict['date'] = post.get('date')
        current_post_dict['updated'] = post.get('updated')
        current_post_dict['post_url'] = post.get('post_url')
        current_post_dict['blog_name'] = post.get('blog_name')
        current_post_dict['timestamp'] = post.get('timestamp')
        if 'trail' in post.keys() and len(post['trail']) > 0: # trail is not empty, it's a reblog
            #FIXME: put this in a trail subdictionary
            current_post_dict['reblogged_blog_name'] = post['trail'][0]['blog']['name']
            current_post_dict['remote_id'] = int(post['trail'][0]['post']['id'])
            current_post_dict['remote_content'] = post['trail'][0]['content_raw'].replace('\n', '')
        else: #trail is an empty list
            current_post_dict['reblogged_blog_name'] = None
            current_post_dict['remote_id'] = None
            current_post_dict['remote_content'] = None
            pass
        current_post_dict['photos'] = []
        if 'photos' in post.keys():
            for item in range(0, len(post['photos'])):
                current_post_dict['photos'].append(post['photos'][item]['original_size']['url'])

        update.trimmed_posts_list.append(current_post_dict)

    t1 = time.time()
    print('Building list of posts took %.2f ms' % (1000*(t1-t0)))

#     for post in update.trimmed_posts_list:
#         print("===============================\n\
# POST number: " + str(update.trimmed_posts_list.index(post)))
#         for key, value in post.items():
#             print("key: " + str(key) + "\nvalue: " + str(value) + "\n--")

    return update
























if __name__ == "__main__":
    client = TumblrRestClient("f4jkODxWIhUuu5THsAIRvbtiGc6bJsJkxGm3tioz5bEy7xLnpe", "xuFrojPnBoIMFJnCJnGSInnQLacRobkVfTLuOWgWG11Xfu9ehl")
    # print(client.blog_info("videogame-fantasy"))
    # print(client.posts("videogame-fantasy"))
