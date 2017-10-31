from datetime import datetime, timedelta
from json import loads
from time import time
from collections import deque, OrderedDict
from base64 import b64encode
import requests
from graph_generator import generate_graph
# import matplotlib.pyplot as plt

API_KEY = 'EAACEdEose0cBAAHkVLbW94CEDLC0E5UwzwrMZC73Te6rsI6zZAKI7rihUA3MAQ1KGKQ7JHFVgJTJo8TZBzZAmVnQZBNSIThzZCho6UVFhbbCFsiAiZBpafCQ8UJ88wxQvIYr0IX6rAQDlB4xtDDdnDhqDSEMw8F08Qo4BU1CsYJlrPTjuTGOVlaN7hwNQ3Ori4ZD'
# ID = 10151775534413086
ID = 10155660730655097
BASE_URL = 'https://graph.facebook.com/v2.10/'
FACEBOOK_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S%z'
USAGE_THRESHOLD = 80
BUCKET_SIZE = 30
BATCH_SIZE = 1000

url = '%s%s/comments' % (BASE_URL, ID)


class GetFBTimeseries(object):
    def __init__(self):
        self.delay = 0
        self.usage = [0, 0, 0]
        self.__total = None
        self.time_series = {}
        self.__tokens = deque()
        self.__duplicates_check = set()
        self.__count = 0

    @property
    def total(self):
        if self.__total is None:
            self.__total = self.get_comment_count()
        return self.__total

    def generate_tokens(self):
        total = self.total+1
        while total>0:
            token_string = '%s' % total
            token_bytes = b64encode(token_string.encode()).replace(b'=', b'ZD')
            token = token_bytes.decode()
            self.__tokens.appendleft(token)
            total -= BATCH_SIZE




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
        count = data.get('summary', {}).get('total_count', False)
        return count

    def get_posts(self):
        self.generate_tokens()
        params = {
            'access_token': API_KEY,
            'limit': BATCH_SIZE,
        }
        poll_continue = True
        delay = 0
        start = time()
        # params['after'] = self.__tokens.pop()
        while poll_continue:
            x = requests.get(url, params=params)
            try:
                data = loads(x.content)
            except ValueError as e:
                return False
            # next_token = data.get('paging', {}).get('cursors',{}).get('after')
            try:
                next_token = self.__tokens.pop()
            except IndexError:
                poll_continue = False
            else:
                print(next_token)
            if len(data.get('data', [])) == 0:
                poll_continue = False
            else:
                print('This batch is %s long'%len(data.get('data', [])))
                self.__count += len(data.get('data', []))
                self.kick_the_bucket(data.get('data', []))
            params['after'] = next_token
            self.check_app_usage(x)
            if self.__count % 1000 == 0:
                delta = time() - start
                start = time()
                print('Count: %s, Time for 1k: %ss' % (count, delta))
                print('Delay: %s' % delay)
                print('Current usage: calls: %s cpu: %s time: %s' % tuple(self.usage))
        print('Total records processed: %s' % self.__count)
        return self.time_series


    def kick_the_bucket(self, records):

        # Yeah, I know that it's more flexible to use timestamp etc, but resource-wise this approach is easier
        # because converting each datetime to timestamp, dividing... etc is much less efficient
        duplicates = 0
        for record in records:
            rec_id = record.get('id')
            if rec_id not in self.__duplicates_check:
                raw_time = record.get('created_time')
                parsed_time = datetime.strptime(raw_time, FACEBOOK_DATE_FORMAT)
                bucket_minutes = parsed_time.minute - (parsed_time.minute % BUCKET_SIZE)
                bucket_time = parsed_time.replace(minute=bucket_minutes, second=0, microsecond=0)
                self.time_series[bucket_time] = self.time_series.get(bucket_time, 0) + 1
                self.__duplicates_check.add(rec_id)
            else:
                duplicates += 1
        self.__count -= duplicates
        print('Found duplicates : %s' % duplicates)

    def zerofill_timeseries(self):
        start = sorted(self.time_series)[0]
        finish = sorted(self.time_series)[-1]
        delta = timedelta(minutes=BUCKET_SIZE)
        cursor = start
        while cursor < finish:
            if cursor not in self.time_series:
                self.time_series[cursor] = 0
            cursor += delta


    def format_time_series(self):
        self.zerofill_timeseries()
        matrix = []
        for dt, value in sorted(self.time_series.items()):
            str_date = dt.strftime(FACEBOOK_DATE_FORMAT)
            matrix.append(
                (str_date, value, '#EAA228', value)
            )
        return matrix



    def check_app_usage(self, request):
        header = request.headers.get('x-app-usage', '{}')
        try:
            data = loads(header)
        except ValueError as e:
            return 0
        correct_delay = False
        for val in data.values():
            if val >= USAGE_THRESHOLD:
                correct_delay = True
        self.usage[0] = data['call_count']
        self.usage[1] = data['total_cputime']
        self.usage[2] = data['total_time']
        if not correct_delay:
            return False
        if self.delay == 0:
            self.delay = 0.1
        else:
            self.delay *= 2


processor = GetFBTimeseries()

count = processor.get_comment_count()
print("Initial count: %s" % count)
processor.get_posts()
matrix = processor.format_time_series()
generate_graph(matrix)

