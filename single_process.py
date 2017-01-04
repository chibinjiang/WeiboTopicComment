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

test_curl = "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=3927488604510131&from=singleWeiBo&__rnd=1483499772595' -H 'Cookie: ALF=1486091764; SUB=_2A251aBikDeTxGeNH41cU-C7KyT-IHXVWkrjsrDV8PUJbkNANLUr5kW1854Q93WFhoIB4w_cOfi3lqu4yxw..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhdlIvWVGaaClSqp2YKeY5M5JpX5oz75NHD95Qf1KnfSKn7Soz0Ws4Dqcj_i--fiKn0i-zfi--ci-82iKnNi--fiK.ciKnEi--Xi-iFi-i2i--NiK.0iKLh; _T_WM=46b6a8fd1a5b7545822b9755f7302b90; YF-Page-G0=ed0857c4c190a2e149fc966e43aaf725; YF-Ugrow-G0=56862bac2f6bf97368b95873bc687eef' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/2488235244/Dbx6siVi3?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed"


def single_process():
    dao = WeiboCommentWriter(USED_DATABASE)
    cache = redis.StrictRedis(**USED_REDIS)
    # job = 'http://weibo.com/3263141904/EofwecJFA'
    job = 'http://weibo.com/2188159454/Eparc7X9l'
    # job = 'http://weibo.com/5245731417/Eofv3eeXI'
    for _ in range(10):
        if "||" not in job:  # init comment url
            spider = WeiboCommentSpider(job, '', WEIBO_ACCOUNT_PASSWD, timeout=20)
            spider.use_abuyun_proxy()
            spider.add_request_header()
            # spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
            spider.use_cookie_from_curl(test_curl)
            # spider.use_cookie_from_curl(test_curl[0])
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
            # spider.use_cookie_from_curl(test_curl[0])
            status = spider.gen_html_source(raw=True)  # get raw response text
            # import ipdb; ipdb.set_trace()
            results = spider.parse_comment_info(uri, cache)
            # for res in results:
            #     dao.insert_comment_into_db(res)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
