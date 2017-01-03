#coding=utf-8
import os
import sys
import time
import redis
import pickle
import random
import traceback
from datetime import datetime as dt
import multiprocessing as mp
from requests.exceptions import ConnectionError
from zc_spider.weibo_utils import RedisException
from zc_spider.weibo_config import (
    MANUAL_COOKIES,
    WEIBO_ERROR_TIME, WEIBO_ACCESS_TIME,
    WEIBO_ACCOUNT_PASSWD, WEIBO_CURRENT_ACCOUNT,
    TOPIC_URL_CACHE, TOPIC_INFO_CACHE,
    QCLOUD_MYSQL, OUTER_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS
)
from weibo_comment_spider import WeiboCommentSpider
from weibo_comment_writer import WeiboCommentWriter
reload(sys)
sys.setdefaultencoding('utf-8')

if os.environ.get('SPIDER_ENV') == 'test':
    print "*"*10, "Run in Test environment"
    USED_DATABASE = OUTER_MYSQL
    USED_REDIS = LOCAL_REDIS
elif 'centos' in os.environ.get('HOSTNAME'): 
    print "*"*10, "Run in Qcloud environment"
    USED_DATABASE = QCLOUD_MYSQL
    USED_REDIS = QCLOUD_REDIS
else:
    raise Exception("Unknown Environment, Check it now...")

test_curl = "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059411661694679&from=singleWeiBo&__rnd=1483412126332' -H 'Cookie: ALF=1486002807; SUB=_2A251b30nDeTxGeNH41AQ-S3EzT-IHXVWkANvrDV8PUJbkNANLWX6kW1ggDSG5-LMedvrsK1Y1bIpphpIIA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFN5399i.PqWgYa6LN3vbAU5JpX5oz75NHD95Qf1KnEeK.01hq0Ws4Dqcj_i--ci-z7iKysi--fiK.7i-8hi--fiKy8iKLWi--fi-2ciKnEi--ciKyWiKLF; _T_WM=522e01eb0b69f7bfc444d219ddb399dc; _s_tentry=-; Apache=4496746608072.71.1483410811472; SINAGLOBAL=4496746608072.71.1483410811472; ULV=1483410811503:1:1:1:4496746608072.71.1483410811472:; YF-V5-G0=0baaa8de04b7d4d04b249e7bb109f469; YF-Page-G0=1ac418838b431e81ff2d99457147068c; YF-Ugrow-G0=3a02f95fa8b3c9dc73c74bc9f2ca4fc6' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1644088831/EoVzg76Rx?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed"

def single_process():
    dao = WeiboCommentWriter(USED_DATABASE)
    cache = redis.StrictRedis(**USED_REDIS)
    # job = 'http://weibo.com/3263141904/EofwecJFA'
    job = 'http://weibo.com/1644088831/EoVzg76Rx'
    # job = 'http://weibo.com/5245731417/Eofv3eeXI'
    for _ in range(10):
        if "||" not in job:  # init comment url
            spider = WeiboCommentSpider(job, '', WEIBO_ACCOUNT_PASSWD, timeout=20)
            spider.use_abuyun_proxy()
            spider.add_request_header()
            # spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
            spider.use_cookie_from_curl(test_curl)
            status = spider.gen_html_source()
            xhr_url = spider.gen_xhr_url()  # xhr_url contains ||
            if xhr_url:
                print xhr_url
                job = xhr_url
        else:  # http://num/alphabet||http://js/v6
            uri, xhr = job.split('||')
            spider = WeiboCommentSpider(xhr, '', WEIBO_ACCOUNT_PASSWD, timeout=20)
            spider.use_abuyun_proxy()
            spider.add_request_header()
            # spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
            spider.use_cookie_from_curl(test_curl)
            status = spider.gen_html_source()
            results = spider.parse_comment_info(uri, cache)
            # for res in results:
            #     dao.insert_comment_into_db(res)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
