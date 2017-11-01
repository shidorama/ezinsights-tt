#!/usr/bin/env python
import asyncio
import logging
import socket
from base64 import b64encode
from collections import deque
from datetime import datetime, timedelta
from json import loads
from time import time
from typing import Dict, Union, List

import aiohttp
import fire
import plotly
import requests
from requests import Request

SIZE_HOUR = 0
SIZE_6HOURS = 1
SIZE_12HOURS = 2
SIZE_DAY = 3
SIZE_MONTH = 4  # Not working

logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# BIG ID: 10151775534413086, SMALL: 10155660730655097
BASE_URL = 'https://graph.facebook.com/v2.10/'
FACEBOOK_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S%z'
USAGE_THRESHOLD = 80
BUCKET_SIZE = SIZE_DAY
BATCH_SIZE = 1000
MAX_RETRY = 5


class GetFBTimeseries(object):
    def __init__(self, object_id: int, access_token: str) -> None:
        self.delay = 0
        self.usage = [0, 0, 0]
        self.__total = None
        self.time_series = {}
        self.__tokens = deque()
        self.__exec_queue = set()
        self.__duplicates_check = set()
        self.__count = 0
        self.__id = object_id
        self.__access_token = access_token
        self.__url = '%s%s/comments' % (BASE_URL, self.__id)

    @staticmethod
    def get_bucket_slot(raw_time: str) -> datetime:
        """determine under which key this result should be stored based on bucket size

        :param raw_time: timestamp string from facebook
        :return: bucket key
        """
        parsed_time = datetime.strptime(raw_time, FACEBOOK_DATE_FORMAT)
        params = {
            'second': 0,
            'microsecond': 0,
            'minute': 0
        }
        if BUCKET_SIZE == SIZE_6HOURS:
            params['hour'] = parsed_time.hour - (parsed_time.hour % 6)
        elif BUCKET_SIZE == SIZE_12HOURS:
            params['hour'] = parsed_time.hour - (parsed_time.hour % 6)
        elif BUCKET_SIZE == SIZE_DAY:
            params['hour'] = 0
        elif BUCKET_SIZE == SIZE_MONTH:
            params['hour'] = 0
            params['day'] = 1

        bucket_time = parsed_time.replace(**params)
        return bucket_time

    @property
    def total(self) -> int:
        """gets total amount of comments as reported by facebook

        :return:
        """
        if self.__total is None:
            self.__total = self.get_comment_count()
        return self.__total

    def generate_tokens(self) -> None:
        """Generates queue of tokens for use by comments count get

        :return:
        """
        total = self.total + 1
        while total > 0:
            token_string = '%s' % total
            token_bytes = b64encode(token_string.encode()).replace(b'=', b'ZD')
            token = token_bytes.decode()
            self.__tokens.appendleft(token)
            total -= BATCH_SIZE

    def get_comment_count(self) -> Union[int, bool]:
        params = {
            'access_token': self.__access_token,
            'summary': True
        }
        x = requests.get(self.__url, params=params)
        if x.status_code != 200:
            raise ConnectionError('Server returned non 200 response. Check your token.')
        try:
            data = loads(x.content)
        except ValueError as e:
            return False
        count = data.get('summary', {}).get('total_count', False)
        return count

    async def get_posts(self) -> Dict[datetime, int]:
        """Iterates over tokens and calls actual comment fetcher

        :return:
        """
        poll_continue = True
        delay = 0
        tasks = []
        conn = aiohttp.TCPConnector(
            family=socket.AF_INET,
            verify_ssl=False,
        )
        async with aiohttp.ClientSession(loop=loop, connector=conn) as session:
            self.generate_tokens()
            for token in self.__tokens:
                task = asyncio.ensure_future(self.get_comments_batch(token, session))
                tasks.append(task)
            await asyncio.wait(tasks)

        logger.info('Total records processed: %s' % self.__count)
        return self.time_series

    async def get_comments_batch(self, token, session: aiohttp.ClientSession, retry: int = 0) -> Union[bool, None]:
        """fetches comments by token and the sends them to processing

        :param token:
        :param session:
        :param retry:
        :return:
        """
        start = time()
        params = {
            'access_token': self.__access_token,
            'limit': BATCH_SIZE,
            'after': token
        }
        await asyncio.sleep(self.delay)
        try:
            async with session.get(self.__url, params=params) as req:
                data = await req.json()
                if len(data.get('data', [])) == 0:
                    if 'error' in data:
                        self.process_error(req.status, req.headers, data)
                    logger.warning('No data!')
                    return False
                else:
                    logger.debug('This batch is %s long' % len(data.get('data', [])))
                    self.__count += len(data.get('data', []))
                    self.push_to_bucket(data.get('data', []))
                self.check_app_usage(req)
        except OSError as e:
            if retry < MAX_RETRY:
                logger.warning('Retrying request %s' % retry)
                retry += 1
                asyncio.ensure_future(self.get_comments_batch(token, session, retry))
    @staticmethod
    def process_error(status: int, headers: Dict, data: Dict) -> None:
        e_code = data['error'].get('code')
        e_type = data['error'].get('type')
        e_msg = data['error'].get('message')
        if status == 400:
            if e_code == 100 and e_type == 'OAuthException':
                logger.warning('Some comments are unavailible. Probably because there are more than 24k')

    def push_to_bucket(self, records: List[Dict]) -> None:
        """gets data into timeseries storage

        :param records:
        :return:
        """
        duplicates = 0
        for record in records:
            rec_id = record.get('id')
            if rec_id not in self.__duplicates_check:
                raw_time = record.get('created_time')
                bucket_time = self.get_bucket_slot(raw_time)
                self.time_series[bucket_time] = self.time_series.get(bucket_time, 0) + 1
                self.__duplicates_check.add(rec_id)
            else:
                duplicates += 1
        self.__count -= duplicates
        logger.debug('Found duplicates : %s' % duplicates)

    def zerofill_timeseries(self) -> None:
        """fills non existent buckets between start and finish with zeroes
        """
        start = sorted(self.time_series)[0]
        finish = sorted(self.time_series)[-1]
        if BUCKET_SIZE == SIZE_HOUR:
            delta = timedelta(hours=1)
        elif BUCKET_SIZE == SIZE_6HOURS:
            delta = timedelta(hours=6)
        elif BUCKET_SIZE == SIZE_12HOURS:
            delta = timedelta(hours=12)
        elif BUCKET_SIZE == SIZE_DAY:
            delta = timedelta(days=1)
        cursor = start
        while cursor < finish:
            if cursor not in self.time_series:
                self.time_series[cursor] = 0
            cursor += delta

    def format_timeseries(self) -> Dict:
        """prepares timeseries for graph plotting
        """
        logger.info('Timeseries size (pre ZF) is %s' % len(self.time_series))
        self.zerofill_timeseries()
        logger.info('Timeseries size (aftera ZF) is %s' % len(self.time_series))
        matrix = {
            'x': [],
            'y': []
        }
        for dt, value in sorted(self.time_series.items()):
            str_date = dt.strftime(FACEBOOK_DATE_FORMAT)
            matrix['x'].append(value)
            matrix['y'].append(str_date)
        return matrix

    def check_app_usage(self, request: Request) -> None:
        """Gets data from request and checks if values are nearing threshold, and if so -> increases delay

        :param request: request object which is used to get headers from it
        """
        header = request.headers.get('x-app-usage', '{}')
        try:
            data = loads(header)
        except ValueError as e:
            return
        correct_delay = False
        for val in data.values():
            if val >= USAGE_THRESHOLD:
                correct_delay = True
        self.usage[0] = data.get('call_count')
        self.usage[1] = data.get('total_cputime')
        self.usage[2] = data.get('total_time')
        logger.info('Usage %s-%s-%s' % tuple(self.usage))
        if not correct_delay:
            return
        if self.delay == 0:
            self.delay = 0.1
        else:
            self.delay *= 2


def generate_graph(data: Dict, filename: str) -> None:
    logger.info('Start timeseries render')
    generate_html_graph(data, filename)
    logger.info('Finished rendering')


def generate_html_graph(data: Dict, filename: str) -> None:
    data['orientation'] = 'h'
    graph_data = [plotly.graph_objs.Bar(data)]
    plotly.offline.plot(graph_data, filename=filename)


async def form_graph(id: int, token: str, filename: str):
    processor = GetFBTimeseries(id, token)
    await processor.get_posts()
    matrix = processor.format_timeseries()
    generate_graph(matrix, filename)


def cli_wrapper(**kwargs):
    required_params = {'id', 'token', 'filename'}
    show_help = False
    if set(kwargs.keys()) != required_params:
        show_help = True
    if not show_help:
        id = int(kwargs.get('id'))
        token = kwargs.get('token')
        filename = kwargs.get('filename')
        try:
            loop.run_until_complete(form_graph(id, token, filename))
        finally:
            loop.close()
    else:
        print('Please supply parameters: --id, --token, --filename')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    fire.Fire(cli_wrapper)
