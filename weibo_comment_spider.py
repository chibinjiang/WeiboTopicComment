#-*- coding: utf-8 -*-
# ----------- 评论48992 爬取一个微博的所有评论 --------------

import re
import json
import time
import requests
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from template.weibo_utils import catch_parse_error,extract_chinese_info
from template.weibo_spider import WeiboSpider


class WeiboCommentSpider(WeiboSpider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)
        self.info = {}

    @catch_parse_error((AttributeError, Exception))
    def parse_bozhu_info(self):
        res = {}
        # print '4' * 20, 'Parsing Bozhu info'
        if len(self.page) < 20000:
            return res
        # Parse game is on !!!
        cut_code = '\n'.join(self.page.split('\n')[100:])
        elminate_white = re.sub(r'\\r|\\t|\\n', '', cut_code)
        elminate_quote = re.sub(r'\\"', '"', elminate_white)
        short_code = re.sub(r'\\/', '/', elminate_quote)
        
        # The three numbers
        focus_num_match = re.search(r'\<a bpfilter="page_frame"  class="t_link S_txt1" href="http://weibo.com/.*?" \>\<(\w+) class=".+?"\>(\d+)\</(\w+)\>', short_code)
        fans_num_match = re.search(r'\<a bpfilter="page_frame"  class="t_link S_txt1" href="http://weibo.com/.*?relate=fans.*?" \>\<(\w+) class=".+?"\>(\d+)\</(\w+)\>', short_code)
        weibo_num_match = re.search(r'\<a bpfilter="page_frame"  class="t_link S_txt1" href="http://weibo.com/.*?home.*?" \>\<(\w+) class=".+?"\>(\d+)\</(\w+)\>', short_code)

        if focus_num_match and fans_num_match and weibo_num_match:
            self.info['focus_num'] = int(focus_num_match.group(2))
            self.info['fans_num'] = int(fans_num_match.group(2))
            self.info['weibo_num'] = int(weibo_num_match.group(2))
        else:
            return res
        # parse basic info
        info_units = re.findall('\<li class="li_1 clearfix"\>\<\w+ class="pt_title S_txt2"\>(.+?)\</\w+?\>\<\w+? (class|href)=".+?"\>(.*?)\</\w+?\>\</li\>', short_code)
        if not info_units:
            return res
        for unit in info_units:
            attr, _, value = unit
            if '昵称' in attr:
                self.info['nickname'] = value
            elif '真实姓名' in attr:
                self.info['realname'] = value
            elif '所在地' in attr:
                self.info['location'] = value
            elif '性别' in attr:
                self.info['gender'] = value
            elif '性取向' in attr:
                self.info['sex_tendancy'] = value
            elif '感情状况' in attr:
                self.info['emotion'] = value
            elif '生日' in attr:
                self.info['date_of_birth'] = value
            elif '简介' in attr:
                self.info['introduction'] = value
            elif '邮箱' in attr:
                self.info['email'] = value
            elif 'QQ' in attr:
                self.info['qq'] = value
            elif '注册时间' in attr:
                self.info['registration_date'] = value
            elif '博客' in attr:
                self.info['blog_url'] = value
                if 'href' in value and re.search(r'\>(https?://[^\>\<]*?)\<', value):
                    self.info['blog_url'] = re.search(r'\>(https?://[^\>\<]*?)\<', value).group(1)
            elif '个性域名' in attr:
                self.info['domain'] = value
                if 'href' in value and re.search(r'\>(https?://[^\>\<]*?)\<', value):
                    self.info['domain'] = re.search(r'\>(https?://[^\>\<]*?)\<', value).group(1)
            elif '大学' in attr:
                self.info['university'] = extract_chinese_info(value)
            elif '高中' in attr:
                self.info['high_school'] = extract_chinese_info(value)
            elif '标签' in attr:
                self.info['label'] = extract_chinese_info(value)
            elif '公司' in attr:
                self.info['company'] = extract_chinese_info(value)
        # fill other info
        # import ipdb; ipdb.set_trace()
        self.info['uri'] = self.url
        self.info['weibo_user_url'] = '/'.join(self.url.split('/')[:-1])
        return self.info
