#-*- coding: utf-8 -*-
# ----------- 评论48992 爬取一个微博的所有评论 --------------

import re
import json
import time
import pickle
import requests
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from zc_spider.weibo_utils import catch_parse_error,extract_chinese_info
from zc_spider.weibo_spider import WeiboSpider
from zc_spider.weibo_config import COMMENT_JOBS_CACHE, COMMENT_RESULTS_CACHE

class WeiboCommentSpider(WeiboSpider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)
        self.info = {}

    def gen_xhr_url(self):
        xhr_url = "http://weibo.com/aj/v6/comment/big?ajwvr=6&id=%s&__rnd=%d"
        mid_regex = re.search(r'mid=(\d+)', self.page)
        mid = mid_regex.group(1) if mid_regex else ''
        if mid:
            return "%s||%s" % (self.url, xhr_url % (mid, int(time.time()*1000)))
        return False

    @catch_parse_error((AttributeError, IndexError, KeyError,  Exception))
    def parse_comment_info(self, uri, rconn):
        comment_list = []; current_page = 1
        # print '4' * 20, 'Parsing Bozhu info'
        # if len(self.page) < 20000:
        #     return comment_list
        # Parse game is on !!!
        data = json.loads(self.page)
        if data['code'] != '100000':
            print "Code: %s --> message: %s" % (data['code'], data['msg'])
            return comment_list
        if data['data']['count'] == 0:
            print 'No any comment'
            return comment_list
        totalpage = int(data['data']['page']['totalpage'])
        print '%s has %d comment pages.' % (uri, totalpage)
        if "page" not in self.url and totalpage > 1:
            for i in range(2, totalpage):
                temp_url = self.url + "&page=%d" % i
                rconn.rpush(COMMENT_JOBS_CACHE, "%s||%s" % (uri,temp_url))
        else:
            page_regex = re.search(r'page=(\d+)', self.url)
            current_page = page_regex.group(1) if page_regex else 1
        parser = bs(data['data']['html'], 'html.parser')
        all_comments = parser.find_all('div', attrs={'class': 'list_li S_line1 clearfix'})
        if not all_comments:
            return comment_list
        for comment in all_comments:
            comm_info = {}
            like_div = comment.find(attrs={'node-type': 'like_status'})
            like_regex = re.search(r'(\d+)', like_div.text) if like_div else None
            comm_info['like_num'] = like_regex.group(1) if like_regex else '0'

            protrait_div = comment.find('div', attrs={'class': 'WB_face W_fl'})
            user_link_regex = re.search(r'a href="(http://weibo.com/\w+?)"', str(protrait_div))
            comm_info['user_link'] = user_link_regex.group(1) if user_link_regex else ''
            user_id_regex = re.search(r'usercard="id=(\d+?)"', str(protrait_div))
            comm_info['usercard'] = user_id_regex.group(1) if user_id_regex else ''
            nickname_regex = re.search(r'alt="(.+?)"', str(protrait_div))
            comm_info['nickname'] = nickname_regex.group(1) if nickname_regex else ''
            img_regex = re.search(r'src="(http://.+?\.jpg)"', str(protrait_div))
            comm_info['image_link'] = img_regex.group(1) if img_regex else ''
            date_div = comment.find('div', attrs={'class': 'WB_from S_txt2'})
            comm_info['sub_date'] = date_div.text if date_div else ''
            text_div = comment.find('div', attrs={'class': 'WB_text'})
            # comm_info['text'] = text_div.comments[-1][1:] if text_div else ''
            text = text_div.text
            if text and text.find('：') > 0:
                comm_info['text'] = re.sub(r'.{0,2}@.+[ :]|#.+#', '', text[text.find('：')+1:])
            comm_info['xhr_path'] = self.url
            comm_info['date'] = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            comm_info['pageno'] = current_page
            comm_info['uri'] = uri
            if len(comm_info) > 5:
                comment_list.append(comm_info)
                rconn.rpush(COMMENT_RESULTS_CACHE, pickle.dumps(comm_info))
        return comment_list

"""
http://weibo.com/3937348351/DlqPUtZdY?type=comment#_rnd1482923279013
http://weibo.com/aj/v6/comment/big?ajwvr=6&id=3951080507146862&page=9&__rnd=1482923256904
fullpath
realpath
theme
middle
createdate
pageno
actionno
actionvalue
bucketName
uri
weibocomment_author_nickname ok
weibocomment_author_id ok
weibocomment_author_url ok
weibocomment_author_portrait ok
weibocomment_sub_date  ok
weibocomment_content ok
weibocomment_thumbup_num ok
weibocomment_reply_num ok
weibo_url ok
This should has 5 thumbs_up
curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4057794573394110&from=singleWeiBo&__rnd=1482986103022' -H 'Cookie: SINAGLOBAL=7912212257618.43.1478585959985; wb_publish_fist100_5843638692=1; un=jiangzhibinking@outlook.com; TC-Page-G0=9183dd4bc08eff0c7e422b0d2f4eeaec; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEEudpRbe6xPOuSw2tsZmk9aMgkA4eyfYVGYbj9wTjBzpA.; SUHB=0YhB7ruCQvsAnY; ALF=1514361863; SSOLoginState=1482825864; _s_tentry=login.sina.com.cn; Apache=498463536813.9112.1482825872279; ULV=1482825872294:22:9:2:498463536813.9112.1482825872279:1482719758416; YF-Page-G0=f27a36a453e657c2f4af998bd4de9419; TC-Ugrow-G0=968b70b7bcdc28ac97c8130dd353b55e; TC-V5-G0=866fef700b11606a930f0b3297300d95; YF-V5-G0=c998e7c570da2f8537944063e27af755; YF-Ugrow-G0=169004153682ef91866609488943c77f; SUB=_2AkMvP_eOf8NhqwJRmPoSym_kb4h_zgrEieLBAH7sJRMxHRl-yT9kqkohtRBOyc7mjoYoGFdurmsx154WlnwNew..; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WFXAiKwTsHHk10mqD21nO-O; WBtopGlobal_register_version=202d69f18939069a; UOR=,,zhiji.heptax.com' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/5245731417/Eofv3eeXI?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed
"""
