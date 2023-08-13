import time
import requests
from functools import wraps

# Rate limit settings
req_freq = 1  # 1 request per second
req_seq = 0  # sequence of successful requests
rl_last_run: float

# A few settings to make our requests more robust
requests.DEFAULT_RETRIES = 10
s = requests.session()
s.keep_alive = False


def rate_limited(freq=float('inf')):
    """
    Apply a rate limit to a function
    :param freq: rate limit to be applied in Hertz (number per second)

    Example usage:
    @rate_limited(0.5)
    def my_func():
        # do something

    Would allow my_func() to run at 0.5 Hertz, or once every 2 seconds.
    """

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            global rl_last_run
            try:
                diff = time.time() - rl_last_run
            except NameError:
                diff = float('inf')  # Never run, so infinite diff
            if diff < 1.0 / float(freq):
                time.sleep(1.0 / freq - diff)
            rl_last_run = time.time()
            return fn(*args, **kwargs)

        return wrapper

    return decorator


@rate_limited(req_freq)
def rate_limited_req(method, *args, **kwargs):
    """ Retry a request if we hit the rate limit """
    try:
        resp = requests.request(method, *args, **kwargs, timeout=5)
    except TimeoutError:
        # Try again with a very long timeout
        time.sleep(2)
        resp = requests.request(method, *args, **kwargs, timeout=30)

    global req_freq, req_seq
    if resp.status_code == 429:
        # if we've hit the rate limit, adjust the global limit and retry
        req_freq *= 0.8
        req_seq = 0
        return rate_limited_req(method, *args, **kwargs)

    # Creep the rate frequency back up if we've had a few successes
    if req_seq % 5 == 0:
        req_freq *= 1.1
    req_seq += 1
    return resp
