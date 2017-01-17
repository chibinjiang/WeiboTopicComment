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

test_curl = "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=3854401767012080&from=singleWeiBo&__rnd=1484620553601' -H 'Cookie: SINAGLOBAL=7912212257618.43.1478585959985; _T_WM=03e781554acf9dd24f1be01327a60a32; YF-Page-G0=d0adfff33b42523753dc3806dc660aa7; _s_tentry=-; Apache=9751347814485.37.1483668519299; ULV=1483668519511:25:3:3:9751347814485.37.1483668519299:1483508239455; YF-Ugrow-G0=8751d9166f7676afdce9885c6d31cd61; SSOLoginState=1483668708; YF-V5-G0=a9b587b1791ab233f24db4e09dad383c; TC-V5-G0=31f4e525ed52a18c5b2224b4d56c70a1; TC-Page-G0=4e714161a27175839f5a8e7411c8b98c; login_sid_t=3695840ac390dab8a751b9537410a3a9; WBtopGlobal_register_version=c689c52160d0ea3b; un=jiangzhibinking@outlook.com; wvr=6; TC-Ugrow-G0=e66b2e50a7e7f417f6cc12eec600f517; wb_g_upvideo_5843638692=1; UOR=,,login.sina.com.cn; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEEkTuUwIMaUEtyaEaq8xQ38Lnx6P-M9cTmvwHr2uuiAh0.; SUB=_2A251efKKDeTxGeNG71EX8ybKwj6IHXVWD2NCrDV8PUJbmtANLW_4kW9K-NJZe-K1EdxZ-S3i2C1MraoLTg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5o2p5NHD95Qf1hB0SoeRSo.EWs4Dqcj6i--ciK.Ni-27i--ciKnRiK.pi--Xi-z4iKyFi--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; SUHB=0TPFsv54MkhW8w; ALF=1516117757' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/2803301701/CmPeMtqa4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed"


def single_process():
    dao = WeiboCommentWriter(USED_DATABASE)
    cache = redis.StrictRedis(**USED_REDIS)
    job = 'http://weibo.com/2803301701/CmPeMtqa4'
    # job = 'http://weibo.com/3215951873/DqCZTAD8J'
    # job = 'http://weibo.com/5245731417/Eofv3eeXI'
    # for _ in range(10):
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
        spider.use_cookie_from_curl(test_curl)
        status = spider.gen_html_source(raw=True)  # get raw response text
        # import ipdb; ipdb.set_trace()
        results = spider.parse_comment_info(uri, cache)
        for res in results:
            dao.insert_comment_into_db(res)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
