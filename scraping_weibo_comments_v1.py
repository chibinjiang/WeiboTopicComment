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

test_curl = "curl 'http://weibo.com/1791434577/info' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: _T_WM=091a7f8021abc37974cfbb8fc47e6ba3; ALF=1484106233; SUB=_2A251SmypDeTxGeNG71EX8ybKwj6IHXVWtXThrDV8PUJbkNAKLUb_kW1_1M3WyDkt9alEMQg5hlJuRoq9kg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5oz75NHD95Qf1hB0SoeRSo.EWs4Dqcj6i--ciK.Ni-27i--ciKnRiK.pi--Xi-z4iKyFi--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; TC-Ugrow-G0=968b70b7bcdc28ac97c8130dd353b55e; wvr=6; TC-V5-G0=7975b0b5ccf92b43930889e90d938495; TC-Page-G0=4c4b51307dd4a2e262171871fe64f295' -H 'Connection: keep-alive' --compressed"
CURRENT_ACCOUNT = ''


def init_current_account(cache):
    print 'Initializing weibo account'
    global CURRENT_ACCOUNT
    CURRENT_ACCOUNT = cache.hkeys(MANUAL_COOKIES)[0]
    print '1', CURRENT_ACCOUNT
    cache.set(WEIBO_CURRENT_ACCOUNT, CURRENT_ACCOUNT)
    cache.set(WEIBO_ACCESS_TIME, 0)
    cache.set(WEIBO_ERROR_TIME, 0)
    

def switch_account(cache):
    global CURRENT_ACCOUNT
    if cache.get(WEIBO_ERROR_TIME) and int(cache.get(WEIBO_ERROR_TIME)) > 99:  # error count
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), 'Swithching weibo account'
        expired_account = cache.get(WEIBO_CURRENT_ACCOUNT)
        access_times = cache.get(WEIBO_ACCESS_TIME)
        error_times = cache.get(WEIBO_ERROR_TIME)
        print "Account(%s) access %s times but failed %s times" % (expired_account, access_times, error_times)
        cache.hdel(MANUAL_COOKIES, expired_account)
        if len(cache.hkeys(MANUAL_COOKIES)) == 0:
            raise RedisException('All Weibo Accounts were run out of')
        else:
            new_account = cache.hkeys(MANUAL_COOKIES)[0]
        # init again
        cache.set(WEIBO_CURRENT_ACCOUNT, new_account)
        cache.set(WEIBO_ACCESS_TIME, 0)
        cache.set(WEIBO_ERROR_TIME, 0)
        CURRENT_ACCOUNT = new_account
    else:
        CURRENT_ACCOUNT = cache.get(WEIBO_CURRENT_ACCOUNT)


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
            switch_account(cache)
            cache.incr(WEIBO_ACCESS_TIME)
            if "||" not in job:  # init comment url
                spider = WeiboCommentSpider(job, CURRENT_ACCOUNT, WEIBO_ACCOUNT_PASSWD, timeout=20)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, CURRENT_ACCOUNT))
                # spider.use_cookie_from_curl(test_curl)
                status = spider.gen_html_source()
                xhr_url = spider.gen_xhr_url()  # xhr_url contains ||
                if xhr_url:
                    cache.lpush(COMMENT_JOBS_CACHE, xhr_url)
            else:  # http://num/alphabet||http://js/v6
                uri, xhr = job.split('||')
                spider = WeiboCommentSpider(xhr, CURRENT_ACCOUNT, WEIBO_ACCOUNT_PASSWD, timeout=20)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, CURRENT_ACCOUNT))
                # spider.use_cookie_from_curl(test_curl)
                status = spider.gen_html_source()
                spider.parse_comment_info(uri, cache)
        except RedisException as e:
            print str(e)
            break
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            cache.incr(WEIBO_ERROR_TIME)
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
    # Producer is on !!!
    job_pool = mp.Pool(processes=1,
        initializer=generate_info, initargs=(r, ))
    result_pool = mp.Pool(processes=1, 
        initializer=write_data, initargs=(r, ))

    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
    try:
        job_pool.close()
        result_pool.close()
        job_pool.join()
        result_pool.join()
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
