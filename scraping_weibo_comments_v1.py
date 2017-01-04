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
    COMMENT_JOBS_CACHE, COMMENT_RESULTS_CACHE,
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

test_curl = [
    "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059411661694679&from=singleWeiBo&__rnd=1483412126332' -H 'Cookie: ALF=1486002807; SUB=_2A251b30nDeTxGeNH41AQ-S3EzT-IHXVWkANvrDV8PUJbkNANLWX6kW1ggDSG5-LMedvrsK1Y1bIpphpIIA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFN5399i.PqWgYa6LN3vbAU5JpX5oz75NHD95Qf1KnEeK.01hq0Ws4Dqcj_i--ci-z7iKysi--fiK.7i-8hi--fiKy8iKLWi--fi-2ciKnEi--ciKyWiKLF; _T_WM=522e01eb0b69f7bfc444d219ddb399dc; _s_tentry=-; Apache=4496746608072.71.1483410811472; SINAGLOBAL=4496746608072.71.1483410811472; ULV=1483410811503:1:1:1:4496746608072.71.1483410811472:; YF-V5-G0=0baaa8de04b7d4d04b249e7bb109f469; YF-Page-G0=1ac418838b431e81ff2d99457147068c; YF-Ugrow-G0=3a02f95fa8b3c9dc73c74bc9f2ca4fc6' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1644088831/EoVzg76Rx?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059411661694679&from=singleWeiBo&__rnd=1483412126332' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: ALF=1486005421; SUB=_2A251b2f9DeRxGeNH41AQ9ynMwjqIHXVWkAm1rDV8PUJbkNANLVTmkW03YySKrU1nCIImpcOpdP5nnorfKg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFT2dlj-bqZjqC9LXv98eo25JpX5oz75NHD95Qf1KnEeKMNeh.cWs4Dqcj_i--Xi-zRiKyWi--ci-i2i-88i--Ni-iFi-zXi--ci-i8i-2pi--ci-zfi-zR; _T_WM=19b8b1a7f5726ab2eedf41a9a9b75153; YF-V5-G0=e6f12d86f222067e0079d729f0a701bc; YF-Page-G0=f27a36a453e657c2f4af998bd4de9419' -H 'Connection: keep-alive' --compressed",
    "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059411661694679&from=singleWeiBo&__rnd=1483412126332' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: YF-V5-G0=4d1671d4e87ac99c27d9ffb0ccd1578f; login_sid_t=f6257c41d4c253e63549ccdde6753ef0; YF-Ugrow-G0=9642b0b34b4c0d569ed7a372f8823a8e; WBStorage=194a5e7d191964cc|undefined; _s_tentry=-; ALF=1486005521; SUB=_2A251b2hBDeTxGeNH41YV9ijJyTmIHXVWkAgJrDV8PUJbkNANLUSskW037-hfXGJlSxgw1nykWg4-Dc2bdQ..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9Wh8Jdz5gS-gWxhvzhal8s-w5JpX5oz75NHD95Qf1KnXShqcSKzfWs4Dqcj_i--RiKn0iKnpi--fi-88i-z0i--RiKn0i-2pi--ciKyFiKnfi--fi-2XiKLW' -H 'Connection: keep-alive' --compressed",
    "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059411661694679&from=singleWeiBo&__rnd=1483412126332' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: ALF=1486005574; SUB=_2A251b2gXDeTxGeNH41YV9inPwz2IHXVWkAhfrDV8PUJbkNANLXDnkW2AOHtzwZCvc0n_bNzVfg8Q74X9dg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWcqhaLjq-Gl9w12b3qhi2z5JpX5oz75NHD95Qf1KnXShqNe0npWs4Dqcj_i--RiKyhiKn0i--4iK.Ni-zNi--Xi-zRi-2Xi--Ni-24i-zXi--ciKyFiKLh; _T_WM=440184b51f87a16e829801e752a0154a' -H 'Connection: keep-alive' --compressed",
    "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059411661694679&from=singleWeiBo&__rnd=1483412126332' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: ALF=1486005622; SUB=_2A251b2gmDeTxGeNH41AQ-C7OzDWIHXVWkAhurDV8PUJbkNANLVP8kW1TUw0OmPiIzO_MfzbYScBNDvSotg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5dSCciEoc-YSEP1UYmRgE95JpX5oz75NHD95Qf1KnEeKn7eoM4Ws4Dqcj_i--4iK.Ni-iFi--ci-z0i-2Ei--Ni-zfi-88i--ci-20i-88i--Ni-2Ri-27; _T_WM=bb255caf7a8c4ab801dd26bd1b74afd3' -H 'Connection: keep-alive' --compressed"
]


def generate_info(cache):
    """
    Producer for urls and topics, Consummer for topics
    """
    error_count = 0
    cp = mp.current_process()
    while True:
        res = {}
        if error_count > 999:
            print '>'*10, 'Exceed 1000 times of gen errors', '<'*10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Comment Process pid is %d" % (cp.pid)
        job = cache.blpop(COMMENT_JOBS_CACHE, 0)[1]
        try:
            all_account = cache.hkeys(MANUAL_COOKIES)
            account = random.choice(all_account)
            account = "binking"
            if "||" not in job:  # init comment url
                spider = WeiboCommentSpider(job, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
                # spider.use_cookie_from_curl(random.choice(test_curl))
                status = spider.gen_html_source()
                xhr_url = spider.gen_xhr_url()  # xhr_url contains ||
                if xhr_url:
                    cache.lpush(COMMENT_JOBS_CACHE, xhr_url)
            else:  # http://num/alphabet||http://js/v6
                uri, xhr = job.split('||')
                spider = WeiboCommentSpider(xhr, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
                # spider.use_cookie_from_curl(random.choice(test_curl))
                status = spider.gen_html_source()
                spider.parse_comment_info(uri, cache)
        except RedisException as e:
            print str(e)
            break
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            error_count += 1
            print 'Faild to parse job: ', job
            cache.rpush(COMMENT_JOBS_CACHE, job) # put job back
        

def write_data(cache):
    """
    Consummer for topics
    """
    error_count = 0
    cp = mp.current_process()
    dao = WeiboCommentWriter(USED_DATABASE)
    while True:
        if error_count > 999:
            print '>'*10, 'Exceed 1000 times of write errors', '<'*10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Comment Process pid is %d" % (cp.pid)
        res = cache.blpop(COMMENT_RESULTS_CACHE, 0)[1]
        try:
            dao.insert_comment_into_db(pickle.loads(res))
        except Exception as e:  # won't let you died
            error_count += 1
            print 'Failed to write result: ', pickle.loads(res)
            cache.rpush(COMMENT_RESULTS_CACHE, res)


def add_jobs(target):
    todo = 0
    dao = WeiboCommentWriter(USED_DATABASE)
    for job in dao.read_comment_from_db():  # iterate
        todo += 1
        target.rpush(COMMENT_JOBS_CACHE, job)
    print 'There are totally %d jobs to process' % todo
    return todo


def run_all_worker():
    r = redis.StrictRedis(**USED_REDIS)
    # r.delete(COMMENT_JOBS_CACHE, COMMENT_RESULTS_CACHE)
    if not r.llen(COMMENT_JOBS_CACHE):
        add_jobs(r)
        print "Add jobs DONE, and I quit..."
        return 0
    else:
        print "Redis has %d records in cache" % r.llen(COMMENT_JOBS_CACHE)
    # init_current_account(r)
    job_pool = mp.Pool(processes=4,
        initializer=generate_info, initargs=(r, ))
    result_pool = mp.Pool(processes=2, 
        initializer=write_data, initargs=(r, ))

    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
    try:
        job_pool.close(); result_pool.close()
        job_pool.join(); result_pool.join()
        print "+"*10, "jobs' length is ", r.llen(COMMENT_JOBS_CACHE) 
        print "+"*10, "results' length is ", r.llen(COMMENT_RESULTS_CACHE)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in run all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"
        print "+"*10, "jobs' length is ", r.llen(COMMENT_JOBS_CACHE) 
        print "+"*10, "results' length is ", r.llen(COMMENT_RESULTS_CACHE)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
