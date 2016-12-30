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

test_curl = "curl 'http://weibo.com/1791434577/info' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: _T_WM=091a7f8021abc37974cfbb8fc47e6ba3; ALF=1484106233; SUB=_2A251SmypDeTxGeNG71EX8ybKwj6IHXVWtXThrDV8PUJbkNAKLUb_kW1_1M3WyDkt9alEMQg5hlJuRoq9kg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5oz75NHD95Qf1hB0SoeRSo.EWs4Dqcj6i--ciK.Ni-27i--ciKnRiK.pi--Xi-z4iKyFi--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; TC-Ugrow-G0=968b70b7bcdc28ac97c8130dd353b55e; wvr=6; TC-V5-G0=7975b0b5ccf92b43930889e90d938495; TC-Page-G0=4c4b51307dd4a2e262171871fe64f295' -H 'Connection: keep-alive' --compressed"

def single_process():
    dao = WeiboCommentWriter(USED_DATABASE)
    cache = redis.StrictRedis(**USED_REDIS)
    # job = 'http://weibo.com/3263141904/EofwecJFA'
    job = 'http://weibo.com/5800589742/EokcFdwMT'
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
            for res in results:
                dao.insert_comment_into_db(res)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
