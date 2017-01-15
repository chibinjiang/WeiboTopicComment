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

test_curl = "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059539257612395&__rnd=1483676899106' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: SINAGLOBAL=7912212257618.43.1478585959985; wb_publish_fist100_5843638692=1; wvr=6; _T_WM=03e781554acf9dd24f1be01327a60a32; YF-Page-G0=d0adfff33b42523753dc3806dc660aa7; _s_tentry=-; Apache=9751347814485.37.1483668519299; ULV=1483668519511:25:3:3:9751347814485.37.1483668519299:1483508239455; YF-Ugrow-G0=8751d9166f7676afdce9885c6d31cd61; WBtopGlobal_register_version=c689c52160d0ea3b; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEEvUHF2uToKM-7WlBpLkmhZ8RBzBoq6rkGPr6RQnLxkPM.; SUB=_2A251aoy0DeTxGeNG71EX8ybKwj6IHXVWAfl8rDV8PUNbmtANLXbhkW-Ca4XWBrg6Mlj9Y8JHL6ezeBXp4A..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5K2hUgL.Fo-RShece0nc1Kz2dJLoI0YLxKqL1KMLBK5LxKqL1hnL1K2LxKBLBo.L12zLxK.L1KnLBoeLxKqL1KnL12-LxK-LBo5L1K2LxK-LBo.LBoBt; SUHB=0sqRRqxSCPeB1B; ALF=1484273507; SSOLoginState=1483668708; un=jiangzhibinking@outlook.com; YF-V5-G0=a9b587b1791ab233f24db4e09dad383c; UOR=,,zhiji.heptax.com' -H 'Connection: keep-alive' --compressed"


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
            all_account = cache.hkeys(COMMENT_COOKIES)
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
                    cache.lpush(COMMENT_JOBS_CACHE, xhr_url)
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
        except RedisException as e:
            print str(e)
            break
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


def add_jobs(target):
    todo = 0
    dao = WeiboCommentWriter(USED_DATABASE)
    for job in dao.read_specified_user():  # iterate
        todo += 1
        if todo > 20:
            break
        target.rpush(COMMENT_JOBS_CACHE, job)
    print 'There are totally %d jobs to process' % todo
    return todo


def run_all_worker():
    r = redis.StrictRedis(**USED_REDIS)
    # r.delete(COMMENT_JOBS_CACHE, COMMENT_RESULTS_CACHE)
    if r.llen(COMMENT_JOBS_CACHE):
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
    print "\n\n" + "%s Began Specified Task" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Totally Specified Task Time Consumed : %d seconds" % (time.time() - start), "*"*10
