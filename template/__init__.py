#coding=utf-8
import os
import re
import time
import requests
import traceback
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from tornado import httpclient, gen, ioloop, queues
from requests.exceptions import (
    ProxyError, Timeout,
    ConnectionError, ConnectTimeout,
)
from weibo_utils import (
    gen_abuyun_proxy, retry,
    handle_proxy_error, handle_sleep
)
from weibo_config import *

exc_list = (IndexError, ProxyError, Timeout, ConnectTimeout, ConnectionError, Exception)


class AsySpider(object):  
    """A simple class of asynchronous spider."""
    def __init__(self, urls, concurrency=10, results=None, **kwargs):
        urls.reverse()
        self.urls = urls
        self.concurrency = concurrency
        self._q = queues.Queue()
        self._fetching = set()
        self._fetched = set()
        if results is None:
            self.results = []

    def fetch(self, url, **kwargs):
        fetch = getattr(httpclient.AsyncHTTPClient(), 'fetch')
        return fetch(url, raise_error=False, **kwargs)

    def handle_html(self, url, html):
        """handle html page"""
        # print(url)
        pass

    def handle_response(self, url, response):
        """inherit and rewrite this method if necessary"""
        if response.code == 200:
            self.handle_html(url, response.body)

        elif response.code == 599:    # retry
            self._fetching.remove(url)
            self._q.put(url)

    @gen.coroutine
    def get_page(self, url):
        try:
            response = yield self.fetch(url)
            #print('######fetched %s' % url)
        except Exception as e:
            print('Exception: %s %s' % (e, url))
            raise gen.Return(e)
        raise gen.Return(response)

    @gen.coroutine
    def _run(self):
        @gen.coroutine
        def fetch_url():
            current_url = yield self._q.get()
            try:
                if current_url in self._fetching:
                    return

                #print('fetching****** %s' % current_url)
                self._fetching.add(current_url)

                response = yield self.get_page(current_url)
                self.handle_response(current_url, response)    # handle reponse

                self._fetched.add(current_url)

                for i in range(self.concurrency):
                    if self.urls:
                        yield self._q.put(self.urls.pop())

            finally:
                self._q.task_done()

        @gen.coroutine
        def worker():
            while True:
                yield fetch_url()

        self._q.put(self.urls.pop())    # add first url

        # Start workers, then wait for the work queue to be empty.
        for _ in range(self.concurrency):
            worker()

        yield self._q.join(timeout=timedelta(seconds=300000))
        try:
            assert self._fetching == self._fetched
        except AssertionError:
            print(self._fetching-self._fetched)
            print(self._fetched-self._fetching)

    def run(self):
        io_loop = ioloop.IOLoop.current()
        io_loop.run_sync(self._run)


class AsyncSpider(AsySpider):
    def handle_html():
        pass


class Spider(object):
    def __init__(self, start_url, curl='', timeout=10, delay=1, proxy={}):
        self.url = start_url
        self.curl = curl
        self.cookie = {}  # self.extract_cookie_from_curl()
        self.post = {}  # self.extract_post_data_from_curl()
        self.headers = {}
        self.timeout = timeout
        self.proxy = proxy
        self.delay = delay
        self.page = ''

    def use_abuyun_proxy(self):
        self.proxy = gen_abuyun_proxy()
    
    def add_request_header(self):
        self.headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            # 'Referer': 'http://weibo.com/sorry?pagenotfound&',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8',
        }

    def extract_cookie_from_curl(self):
        cookie_dict = {}
        tokens = self.curl.split("'")
        if not tokens:
            # curl is empty string
            return cookie_dict
        try:
            for i in range(0, len(tokens)-1, 2):
                # if tokens[i].startswith("curl"):
                #     url = tokens[i+1]
                if "-H" in tokens[i]:
                    attr, value = tokens[i+1].split(": ")  # be careful space
                    if 'Cookie' in attr:
                        cookie_dict[attr] = value
        except Exception as e:
            print "!"*20, "Parsed cURL Failed"
            traceback.print_exc()
        return cookie_dict

    def extract_post_data_from_curl(self):
        """
        Given curl that was cpoied from Chrome, no matter baidu or sogou, 
        parse it and then get url and the data you will post/get with requests
        """
        post_data = {}
        tokens = self.curl.split("'")
        if not tokens:
            # curl is empty string
            return cookie_dict
        try:
            for i in range(0, len(tokens)-1, 2):
                if tokens[i].startswith("curl"):
                    url = tokens[i+1]
                elif "-H" in tokens[i]:
                    attr, value = tokens[i+1].split(": ")  # be careful space
                    post_data[attr] = value
        except Exception as e:
            print "!"*20, "Parsed cURL Failed"
            traceback.print_exc()
        return post_data

    # @catch_network_error(exc_list)
    @retry(exc_list, tries=4, delay=2, backoff=2)
    def gen_html_source(self, method='python'):
        """
        Separate get page and parse page
        """
        if not self.cookie:
            return None
        request_args = {'timeout': self.timeout,
            'cookies': self.cookie, 'proxies': self.proxy,
        }
        if method == 'python':
            # proxy = gen_abuyun_proxy()
            source_code = requests.get(self.url, **request_args).text
        else:
            if '--silent' not in self.curl:
                self.curl += '--silent'
            source_code = os.popen(self.curl).read()
        # parser = bs(source_code, 'html.parser')
        elminate_white = re.sub(r'\\r|\\t|\\n', '', source_code)
        elminate_quote = re.sub(r'\\"', '"', elminate_white)
        elminate_slash = re.sub(r'\\/', '/', elminate_quote)
        handle_sleep(self.delay)
        return elminate_slash.encode('utf8')

