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
    "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=3927488604510131&from=singleWeiBo&__rnd=1483498984091' -H 'Cookie: ALF=1486090296; SUB=_2A251aBNoDeTxGeNH41AT8SvIzT6IHXVWkr0grDV8PUJbkNANLXD6kW0rHUQEwLzlyIQRIisxoZW7GPObXA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWWRKKwdRYEYxXFF2rPYLE75JpX5oz75NHD95Qf1KnEeo2fShqEWs4Dqcj_i--ci-82i-iFi--4iKn0i-i2i--fi-iWiK.fi--Ni-iFiK.0i--ciK.RiK.0; _T_WM=cc1b182fe4bfce02fbbff0aa11c40172; YF-V5-G0=a53c7b4a43414d07adb73f0238a7972e; YF-Page-G0=4c69ce1a525bc6d50f53626826cd2894; YF-Ugrow-G0=ad83bc19c1269e709f753b172bddb094; _s_tentry=-; Apache=7752738435652.491.1483498967984; SINAGLOBAL=7752738435652.491.1483498967984; ULV=1483498968035:1:1:1:7752738435652.491.1483498967984:' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/2488235244/Dbx6siVi3?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=3927488604510131&from=singleWeiBo&__rnd=1483499531544' -H 'Cookie: ALF=1486091514; SUB=_2A251aBerDeTxGeNH41cU9y7OzTmIHXVWkrnjrDV8PUJbkNANLRCmkW0r9fUoT0PP8VXc5K59chs_T85scg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFg8oQ_4kEeo5yf.Qyser_55JpX5oz75NHD95Qf1KnfSKM7eoqfWs4Dqcj_i--ci-zciKnRi--ci-2ciKnci--fiK.fi-2ci--RiKLhiK.0i--fiK.0iK.p; _T_WM=dd277b74e789d17d21c363da940ba329; YF-Page-G0=e3ff5d70990110a1418af5c145dfe402; YF-Ugrow-G0=ad83bc19c1269e709f753b172bddb094; _s_tentry=-; Apache=6979858962659.345.1483499531210; SINAGLOBAL=6979858962659.345.1483499531210; ULV=1483499531299:1:1:1:6979858962659.345.1483499531210:' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/2488235244/Dbx6siVi3' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=3927488604510131&from=singleWeiBo&__rnd=1483499680785' -H 'Cookie: ALF=1486091666; SUB=_2A251aBjDDeTxGeNH41AT8C_LzD-IHXVWkriLrDV8PUJbkNANLVGkkW1kOhzhY5DrTmJ2iXKAosd61WUiwg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhjdOISPHO5vkEqf9d-HpF25JpX5oz75NHD95Qf1KnEeo5pS0M0Ws4Dqcj_i--fiKyhi-iFi--ci-zfiKy8i--fi-zRiKn0i--NiKn4iKyhi--Xi-zRi-i2; _T_WM=102dfeddca40c3db8ad77409dd13bc12; YF-Page-G0=280e58c5ca896750f16dcc47ceb234ed; YF-Ugrow-G0=ad83bc19c1269e709f753b172bddb094' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/2488235244/Dbx6siVi3' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=3927488604510131&from=singleWeiBo&__rnd=1483499772595' -H 'Cookie: ALF=1486091764; SUB=_2A251aBikDeTxGeNH41cU-C7KyT-IHXVWkrjsrDV8PUJbkNANLUr5kW1854Q93WFhoIB4w_cOfi3lqu4yxw..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhdlIvWVGaaClSqp2YKeY5M5JpX5oz75NHD95Qf1KnfSKn7Soz0Ws4Dqcj_i--fiKn0i-zfi--ci-82iKnNi--fiK.ciKnEi--Xi-iFi-i2i--NiK.0iKLh; _T_WM=46b6a8fd1a5b7545822b9755f7302b90; YF-Page-G0=ed0857c4c190a2e149fc966e43aaf725; YF-Ugrow-G0=56862bac2f6bf97368b95873bc687eef' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/2488235244/Dbx6siVi3?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed"
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
            # all_account = cache.hkeys(MANUAL_COOKIES)
            # account = random.choice(all_account)
            account = "binking"
            if "||" not in job:  # init comment url
                spider = WeiboCommentSpider(job, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                # spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
                spider.use_cookie_from_curl(random.choice(test_curl))
                status = spider.gen_html_source()
                xhr_url = spider.gen_xhr_url()  # xhr_url contains ||
                if xhr_url:
                    cache.lpush(COMMENT_JOBS_CACHE, xhr_url)
            else:  # http://num/alphabet||http://js/v6
                uri, xhr = job.split('||')
                spider = WeiboCommentSpider(xhr, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
                spider.use_abuyun_proxy()
                spider.add_request_header()
                # spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
                spider.use_cookie_from_curl(random.choice(test_curl))
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
