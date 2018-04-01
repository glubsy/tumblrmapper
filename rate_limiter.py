import time
import asyncio
import functools
import threading
import collections
import logging
import datetime


class RateLimiter(object):

    """Provides rate limiting for an operation with a configurable number of
    requests for a time period.
    """

    def __init__(self, max_calls, period=1.0, callback=None):
        """Initialize a RateLimiter object which enforces as much as max_calls
        operations on period (eventually floating) number of seconds.
        """
        if period <= 0:
            raise ValueError('Rate limiting period should be > 0')
        if max_calls <= 0:
            raise ValueError('Rate limiting number of calls should be > 0')

        # We're using a deque to store the last execution timestamps, not for
        # its maxlen attribute, but to allow constant time front removal.
        self.calls = collections.deque()

        self.period = period
        self.max_calls = max_calls
        self.callback = callback
        self._lock = threading.Lock()
        self._alock = None

        # Lock to protect creation of self._alock
        self._init_lock = threading.Lock()

    def __call__(self, f):
        """The __call__ function allows the RateLimiter object to be used as a
        regular function decorator.
        """
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            with self:
                return f(*args, **kwargs)
        return wrapped

    def __enter__(self):
        with self._lock:
            # We want to ensure that no more than max_calls were run in the allowed
            # period. For this, we store the last timestamps of each call and run
            # the rate verification upon each __enter__ call.
            if len(self.calls) >= self.max_calls:
                until = time.time() + self.period - self._timespan
                if self.callback:
                    t = threading.Thread(target=self.callback, args=(until,))
                    t.daemon = True
                    t.start()
                sleeptime = until - time.time()
                if sleeptime > 0:
                    time.sleep(sleeptime)
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._lock:
            # Store the last operation timestamp.
            self.calls.append(time.time())

            # Pop the timestamp list front (ie: the older calls) until the sum goes
            # back below the period. This is our 'sliding period' window.
            while self._timespan >= self.period:
                self.calls.popleft()

    @property
    def _timespan(self):
        return self.calls[-1] - self.calls[0]


class AsyncRateLimiter(RateLimiter):

    def _init_async_lock(self):
        with self._init_lock:
            if self._alock is None:
                self._alock = asyncio.Lock()

    async def __aenter__(self):
        if self._alock is None:
            self._init_async_lock()

        with await self._alock:
            # We want to ensure that no more than max_calls were run in the allowed
            # period. For this, we store the last timestamps of each call and run
            # the rate verification upon each __enter__ call.
            if len(self.calls) >= self.max_calls:
                until = time.time() + self.period - self._timespan
                if self.callback:
                    asyncio.ensure_future(self.callback(until))
                sleeptime = until - time.time()
                if sleeptime > 0:
                    await asyncio.sleep(sleeptime)
            return self

    __aexit__ = asyncio.coroutine(RateLimiter.__exit__)


class TooManyRequestsError(Exception):
    def __str__(self):
        return "More than 30 requests have been made in the last five seconds."


class Throttler(object):
    cache = {}

    def __init__(self, max_rate, window, throttle_stop=False, cache_age=1800):
        # Dict of max number of requests of the API rate limit for each source
        self.max_rate = max_rate
        # Dict of duration of the API rate limit for each source
        self.window = window
        # Whether to throw an error (when True) if the limit is reached, or wait until another request
        self.throttle_stop = throttle_stop
        # The time, in seconds, for which to cache a response
        self.cache_age = cache_age
        # Initialization
        self.next_reset_at = dict()
        self.num_requests = dict()

        now = datetime.datetime.now()
        for source in self.max_rate:
            self.next_reset_at[source] = now + datetime.timedelta(seconds=self.window.get(source))
            self.num_requests[source] = 0

    def request(self, source, method, do_cache=False):
        now = datetime.datetime.now()

        # if cache exists, no need to make api call
        key = source + method.func_name
        if do_cache and key in self.cache:
            timestamp, data = self.cache.get(key)
            logging.info('{} exists in cached @ {}'.format(key, timestamp))

            if (now - timestamp).seconds < self.cache_age:
                logging.info('retrieved cache for {}'.format(key))
                return data

        # <--- MAKE API CALLS ---> #

        # reset the count if the period passed
        if now > self.next_reset_at.get(source):
            self.num_requests[source] = 0
            self.next_reset_at[source] = now + datetime.timedelta(seconds=self.window.get(source))

        # throttle request
        def halt(wait_time):
            if self.throttle_stop:
                raise TooManyRequestsError()
            else:
                # Wait the required time, plus a bit of extra padding time.
                time.sleep(wait_time + 0.1)

        # if exceed max rate, need to wait
        if self.num_requests.get(source) >= self.max_rate.get(source):
            logging.info('back off: {} until {}'.format(source, self.next_reset_at.get(source)))
            halt((self.next_reset_at.get(source) - now).seconds)

        self.num_requests[source] += 1
        response = method()  # potential exception raise

        # cache the response
        if do_cache:
            self.cache[key] = (now, response)
            logging.info('cached instance for {}, {}'.format(source, method))

        return response