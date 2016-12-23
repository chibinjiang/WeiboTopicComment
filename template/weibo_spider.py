#coding=utf-8
import os
import re
import json
import time
import base64
import requests
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from requests.exceptions import (
    ProxyError, Timeout,
    ConnectionError, ConnectTimeout,
)
from . import Spider
from weibo_utils import extract_cookie_from_curl, retry   
from weibo_config import *

weibo_ranks = ['icon_member', 'icon_club', 'icon_female', 'icon_vlady', 'icon_pf_male', 'W_icon_vipstyle']
exc_list = (IndexError, ProxyError, Timeout, ConnectTimeout, ConnectionError, Exception)



class WeiboSpider(Spider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        Spider.__init__(self, start_url, timeout=timeout, delay=delay, proxy=proxy)
        self.account = account
        self.password = password
        print 'Parsing %s using Account %s' % (self.url, self.account)

    def read_cookie(self, rconn):
        auth = '%s--%s' % (self.account, self.password)
        if auth in rconn.hkeys(ACTIVATED_COOKIE):
            self.cookie = json.loads(rconn.hget(ACTIVATED_COOKIE, auth))
            return True
        else:
            status = self.gen_cookie(rconn)
            if status:
                return True
        return False

    def use_cookie_from_curl(self, curl):
        # curl = "curl 'http://weibo.com/1791434577/info' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: _T_WM=091a7f8021abc37974cfbb8fc47e6ba3; ALF=1484106233; SUB=_2A251SmypDeTxGeNG71EX8ybKwj6IHXVWtXThrDV8PUJbkNAKLUb_kW1_1M3WyDkt9alEMQg5hlJuRoq9kg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5oz75NHD95Qf1hB0SoeRSo.EWs4Dqcj6i--ciK.Ni-27i--ciKnRiK.pi--Xi-z4iKyFi--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; TC-Ugrow-G0=968b70b7bcdc28ac97c8130dd353b55e; wvr=6; TC-V5-G0=7975b0b5ccf92b43930889e90d938495; TC-Page-G0=4c4b51307dd4a2e262171871fe64f295' -H 'Connection: keep-alive' --compressed"
        self.cookie = extract_cookie_from_curl(curl)

    @retry(exc_list, tries=3, delay=3, backoff=2)
    def gen_cookie(self, rconn):
        """ 
        获取一个账号的Cookie
        Cookie is str
        """
        auth = '%s--%s' % (self.account, self.password)
        # loginURL = "https://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.15)"
        loginURL = "https://login.sina.com/sso/login.php?client=ssologin.js(v1.4.15)"
        username = base64.b64encode(self.account.encode("utf-8")).decode("utf-8")
        postData = {
            "entry": "sso", "gateway": "1",
            "from": "null", "savestate": "30",
            "useticket": "0", "pagerefer": "",
            "vsnf": "1", "su": username,
            "service": "sso", "sp": self.password,
            "sr": "1440*900", "encoding": "UTF-8",
            "cdult": "3", 
            "domain": "sina.com.cn",
            "prelt": "0", "returntype": "TEXT",
        }
        # import ipdb; ipdb.set_trace()
        session = requests.Session()
        r = session.post(loginURL, data=postData, headers=self.headers, proxies=self.proxy)
        jsonStr = r.content.decode("gbk")
        info = json.loads(jsonStr)
        if info["retcode"] == "0":
            # logger.warning("Get Cookie Success!( Account:%s )" % account)
            print "Get Cookie Success!( Account:%s )" % self.account
            self.cookie = session.cookies.get_dict()
            # save cookie into Redis
            rconn.hset(ACTIVATED_COOKIE, auth, json.dumps(self.cookie))
            return True
        else:
            # logger.warning("Failed!( Reason:%s )" % info["reason"])
            print "Failed!( Reason:%s )" % info["reason"]
            time.sleep(2)
            return False

    def update_cookie(self, rconn):
        """ 更新一个账号的Cookie """
        auth = '%s--%s' % (self.account, self.password)
        status = self.gen_cookie(self.account, self.password, rconn)
        if status:
            # logger.warning("The cookie of %s has been updated successfully!" % account)
            print "The cookie of %s has been updated successfully!" % account
            rconn.hset(ACTIVATED_COOKIE, auth, self.cookie)
        else:
            # logger.warning("The cookie of %s updated failed! Remove it!" % accountText)
            print "The cookie of %s updated failed! Remove it!" % accountText
            self.removeCookie(rconn)

    def remove_cookie(self, rconn):
        """ 删除某个账号的Cookie """
        auth = '%s--%s' % (self.account, self.password)
        rconn.hdel(ACTIVATED_COOKIE, auth)
        cookie_num = rconn.hlen(ACTIVATED_COOKIE)
        print "The num of the cookies left is %s" % cookie_num

    @retry(exc_list, tries=2, delay=3, backoff=2)
    def gen_html_source(self):
        """
        Separate get page and parse page
        """
        if not self.cookie:
            return False
        info_response = requests.get(self.url, timeout=self.timeout, headers=self.headers,
            cookies=self.cookie, proxies=self.proxy, allow_redirects=True)
        text = info_response.text.encode('utf8')
        now_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        # catch 404 not found
        if 'pagenotfound' in info_response.url:
            # 'http://weibo.com/sorry?pagenotfound&id_error'
            print "抱歉，你访问的页面地址有误，或者该页面不存在: %s" % self.url
            return 404
            # raise ConnectionError('Freezed: ' + self.url)
        elif 'code=20003' in info_response.url:
            # http://weibo.com/sorry?userblock&is_viewer&code=20003
            print '抱歉，您当前访问的帐号异常，暂时无法访问。(20003): %s' % self.url
            return 20003
        elif len(info_response.history) > 1:
            for redirect in info_response.history:
                if redirect.status_code == 302:
                    print "302 Temporarily Moved: ", self.url
        elif info_response.status_code == 429:
            raise ConnectionError("429 Too many requests: " + self.url)
        elif len(text) == 0:
            print 'Access nothing back: ', self.url
        elif len(text) < 10000:  # Let IndexError disappear
            raise ConnectionError('Blocked: ' + self.url)
        elif text.find('<title>404错误</title>') > 0:  # <title>404错误</title>
            raise ConnectionError('Freezed: ' + self.url)
        elif 16000<len(info_response.text)<18000:
            raise ConnectionError('Short source code: ' + self.url)
        if info_response.status_code == 200:
            self.page = text
        time.sleep(self.delay)
        return True
