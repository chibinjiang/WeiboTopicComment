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

test_curl = "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059539257612395&__rnd=1483676899106' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: SINAGLOBAL=7912212257618.43.1478585959985; wb_publish_fist100_5843638692=1; wvr=6; _T_WM=03e781554acf9dd24f1be01327a60a32; YF-Page-G0=d0adfff33b42523753dc3806dc660aa7; _s_tentry=-; Apache=9751347814485.37.1483668519299; ULV=1483668519511:25:3:3:9751347814485.37.1483668519299:1483508239455; YF-Ugrow-G0=8751d9166f7676afdce9885c6d31cd61; WBtopGlobal_register_version=c689c52160d0ea3b; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEEvUHF2uToKM-7WlBpLkmhZ8RBzBoq6rkGPr6RQnLxkPM.; SUB=_2A251aoy0DeTxGeNG71EX8ybKwj6IHXVWAfl8rDV8PUNbmtANLXbhkW-Ca4XWBrg6Mlj9Y8JHL6ezeBXp4A..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5K2hUgL.Fo-RShece0nc1Kz2dJLoI0YLxKqL1KMLBK5LxKqL1hnL1K2LxKBLBo.L12zLxK.L1KnLBoeLxKqL1KnL12-LxK-LBo5L1K2LxK-LBo.LBoBt; SUHB=0sqRRqxSCPeB1B; ALF=1484273507; SSOLoginState=1483668708; un=jiangzhibinking@outlook.com; YF-V5-G0=a9b587b1791ab233f24db4e09dad383c; UOR=,,zhiji.heptax.com' -H 'Connection: keep-alive' --compressed"

def single_process():
    dao = WeiboCommentWriter(USED_DATABASE)
    cache = redis.StrictRedis(**USED_REDIS)
    job = 'http://weibo.com/1672634524/EoYT3vWkz||http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059539257612395&__rnd=1483676899106'
    # job = 'http://weibo.com/3215951873/DqCZTAD8J'
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
