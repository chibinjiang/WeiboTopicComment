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
    COMMENT_COOKIES,
    WEIBO_ACCOUNT_PASSWD,
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


def generate_info(cache):
    """
    Producer for urls and topics, Consummer for topics
    """
    error_count = 0
    loop_count = 0
    cp = mp.current_process()
    while True:
        res = {}
        loop_count += 1
        if error_count > 999:
            print '>'*10, 'Exceed 1000 times of gen errors', '<'*10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Comment Process pid is %d" % (cp.pid)
        job = cache.blpop(COMMENT_JOBS_CACHE, 0)[1]
        try:
            all_account = cache.hkeys(COMMENT_COOKIES)
            if len(all_account) == 0:
                time.sleep(pow(2, loop_count))
                continue
            account = random.choice(all_account)
            if "||" not in job:  # init comment url
                spider = WeiboCommentSpider(job, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                spider.use_cookie_from_curl(cache.hget(COMMENT_COOKIES, account))
                # spider.use_cookie_from_curl(test_curl)
                status = spider.gen_html_source()
                xhr_url = spider.gen_xhr_url()  # xhr_url contains ||
                if xhr_url:
                    cache.rpush(COMMENT_JOBS_CACHE, xhr_url)
            else:  # http://num/alphabet||http://js/v6
                uri, xhr = job.split('||')
                spider = WeiboCommentSpider(xhr, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                spider.use_cookie_from_curl(cache.hget(COMMENT_COOKIES, account))
                # spider.use_cookie_from_curl(test_curl)
                status = spider.gen_html_source(raw=True)
                spider.parse_comment_info(uri, cache)
        except ValueError as e:
            print e  # print e.message
            time.sleep(1)
            error_count += 1
            cache.rpush(COMMENT_JOBS_CACHE, job) # put job back
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            error_count += 1
            print 'Faild to parse job: ', job
            cache.rpush(COMMENT_JOBS_CACHE, job) # put job back
        time.sleep(2)
        

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
        info = pickle.loads(res)
        try:
            if len(info.get('text', '')) > 0:
                dao.insert_comment_into_db(info)
        except Exception as e:  # won't let you died
            error_count += 1
            print 'Failed to write result: ', info
            cache.rpush(COMMENT_RESULTS_CACHE, res)

def run_all_worker():
    r = redis.StrictRedis(**USED_REDIS)
    # r.delete(COMMENT_JOBS_CACHE, COMMENT_RESULTS_CACHE)
    # if not r.llen(COMMENT_JOBS_CACHE):
    #     add_jobs(r)
    #     print "Add jobs DONE, and I quit..."
    #     return 0
    # else:
    print "Redis has %d records in cache" % r.llen(COMMENT_JOBS_CACHE)
    # init_current_account(r)
    job_pool = mp.Pool(processes=8,
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
    print "\n\n" + "%s Began Scraped Weibo Comments" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Totally Scraped Weibo Comments Time Consumed : %d seconds" % (time.time() - start), "*"*10
