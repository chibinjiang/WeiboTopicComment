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

test_curl = "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059411661694679&from=singleWeiBo&__rnd=1483412126332' -H 'Cookie: ALF=1486002807; SUB=_2A251b30nDeTxGeNH41AQ-S3EzT-IHXVWkANvrDV8PUJbkNANLWX6kW1ggDSG5-LMedvrsK1Y1bIpphpIIA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFN5399i.PqWgYa6LN3vbAU5JpX5oz75NHD95Qf1KnEeK.01hq0Ws4Dqcj_i--ci-z7iKysi--fiK.7i-8hi--fiKy8iKLWi--fi-2ciKnEi--ciKyWiKLF; _T_WM=522e01eb0b69f7bfc444d219ddb399dc; _s_tentry=-; Apache=4496746608072.71.1483410811472; SINAGLOBAL=4496746608072.71.1483410811472; ULV=1483410811503:1:1:1:4496746608072.71.1483410811472:; YF-V5-G0=0baaa8de04b7d4d04b249e7bb109f469; YF-Page-G0=1ac418838b431e81ff2d99457147068c; YF-Ugrow-G0=3a02f95fa8b3c9dc73c74bc9f2ca4fc6' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1644088831/EoVzg76Rx?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed"


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
            if "||" not in job:  # init comment url
                spider = WeiboCommentSpider(job, account, WEIBO_ACCOUNT_PASSWD, timeout=20)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                # spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
                spider.use_cookie_from_curl(test_curl)
                status = spider.gen_html_source()
                xhr_url = spider.gen_xhr_url()  # xhr_url contains ||
                if xhr_url:
                    cache.lpush(COMMENT_JOBS_CACHE, xhr_url)
            else:  # http://num/alphabet||http://js/v6
                uri, xhr = job.split('||')
                spider = WeiboCommentSpider(xhr, account, WEIBO_ACCOUNT_PASSWD, timeout=20)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                # spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
                spider.use_cookie_from_curl(test_curl)
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
