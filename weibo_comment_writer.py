#-*- coding: utf-8 -*-
# ----------- 评论48992 爬取一个微博的所有评论 --------------

import traceback
from datetime import datetime as dt
from template.weibo_writer import DBAccesor, database_error_hunter


class WeiboUserWriter(DBAccesor):

    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    @database_error_hunter
    def insert_new_user_into_db(self, info_dict):
        uri = info_dict['uri']middle = 'default'; 
        bucketName = '博文'; theme = '新浪微博_评论48992'
        insert_comment_sql = """
            INSERT INTO WeiboComment (fullpath, realpath, theme,  middle, 
            createdate, pageno, bucketName,uri, weibocomment_author_nickname, 
            weibocomment_author_id, weibocomment_author_url, weibocomment_author_portrait, 
            weibocomment_sub_date, weibocomment_content, weibocomment_thumbup_num, 
            weibocomment_reply_num, weibo_url)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s 
            FROM DUAL WHERE not exists
            SELECT * FROM WeiboComment WHERE weibo_url = %s 
            AND weibocomment_author_id = %s AND weibocomment_content = %s)
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        if cursor.execute(insert_comment_sql,(
                )):
            print '$'*10, "1. Insert %s SUCCEED." % uri
        conn.commit(); cursor.close(); conn.close()
        return True

    @database_error_hunter
    def read_new_user_from_db(self):
        select_new_user_sql = """
            SELECT weibo_url, createdate FROM weibo w 
            WHERE 1=1   -- Weibo 还没有爬过comment 的列表
            AND createdate  > date_sub(NOW(), INTERVAL '2' DAY)
            AND  not exists(
            SELECT * FROM weibocomment WHERE weibo_url = w.weibo_url)
            AND cast(weibo_thumb_up_num AS signed) > 50
            ORDER BY w.createdate DESC
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_new_user_sql)
        for res in cursor.fetchall():
            yield res[0]