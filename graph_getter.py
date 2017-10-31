from json import loads
from time import time

import requests

API_KEY = 'EAACEdEose0cBACJTdbL0xfEL561ghL8qahaLLb7iXdXODe0ZAMZAtPAZBN2axXrRX2sqcd4rkC135tIzwhPxR1xde3WW9Kf65SwFKJZB2smKq07zMrKH1HrYukdLVaezGsjXMQDGNnM6fhtKSoccGyqR6fXlrHr5ufgLclZBsbHhqOA6s520BaAvW93xFMGMZD'
ID = 10151775534413086
BASE_URL = 'https://graph.facebook.com/v2.10/'
USAGE_THRESHOLD = 80

url = '%s%s/comments'% (BASE_URL, ID)

class GetFBTimeseries(object):

    def get_comment_count(self):
        params = {
            'access_token': API_KEY,
            'summary': True
        }
        x = requests.get(url, params=params)
        try:
            data = loads(x.content)
        except ValueError as e:
            return False
        count = data.get('summary',{}).get('total_count', False)
        return count

    def get_posts(self):
        params = {
            'access_token': API_KEY,
            'limit': 25,
        }
        poll_continue = True
        count = 0
        delay = 0
        start = time()
        while poll_continue:
            x = requests.get(url, params=params)
            try:
                data = loads(x.content)
            except ValueError as e:
                return False
            next_token = data.get('paging', {}).get('after')
            if len(data.get('data',[])) == 0:
                poll_continue = False
            else:
                count += len(data.get('data',[]))
            params['next'] = next_token
            if count % 1000 == 0:
                delta = time() - start
                start = time()
                print('Count: %s, Time for 1k: %ss' % (count, delta))
                print('Delay: %s' % delay)
                print('Current usage')


    def check_app_usage(self, request):
        header = request.headers.get('x-app-usage', '{}')
        try:
            data = loads(header)
        except ValueError as e:
            return 0
        if self.delay == 0:
            delay = 0.1
        else:
            self.delay *= 2
        return delay



count = get_comment_count()
print("Initial count: %s" % count)
get_posts()