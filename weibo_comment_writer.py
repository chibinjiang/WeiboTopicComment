#-*- coding: utf-8 -*-
# ----------- 评论48992 爬取一个微博的所有评论 --------------
import warnings
import traceback
from datetime import datetime as dt
from zc_spider.weibo_writer import DBAccesor, database_error_hunter


warnings.filterwarnings("ignore")


class WeiboCommentWriter(DBAccesor):

    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    @database_error_hunter
    def insert_comment_into_db(self, info):
        xhr_path = info['xhr_path']
        uri = info['uri']; middle = 'default'; 
        bucketName = '博文'; theme = '新浪微博_评论python'
        insert_comment_sql = """
            INSERT INTO WeiboComment (fullpath, realpath, theme,  middle, 
            createdate, pageno, bucketName,uri, weibocomment_author_nickname, 
            weibocomment_author_id, weibocomment_author_url, weibocomment_author_portrait, 
            weibocomment_sub_date, weibocomment_content, weibocomment_thumbup_num, 
            weibocomment_reply_num, weibo_url)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s 
            FROM DUAL WHERE not exists(
            SELECT * FROM WeiboComment WHERE weibo_url = %s 
            AND weibocomment_author_id = %s AND weibocomment_content = %s)
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        try:
            if cursor.execute(insert_comment_sql,(
                xhr_path, xhr_path, theme, middle,
                info['date'], info['pageno'], bucketName, uri, info.get('nickname', ''),
                info.get('usercard', ''), info.get('user_link', ''), info.get('image_link'),
                info.get('sub_date'), info.get('text', ''), info.get('like_num', '0'),
                info.get('reply_num', '0'), uri,
                uri, info.get('usercard', ''), info.get('text', '')
            )):
                print '$'*10, "1. Insert comment %s SUCCEED." % uri
            conn.commit(); cursor.close(); conn.close()
        except Exception as e:
            traceback.print_exc()
            conn.rollback()
            conn.commit(); cursor.close(); conn.close()
            raise Exception(str(e))

    @database_error_hunter
    def read_comment_from_db(self):
        """
            SET NAMES utf8mb4;
            ALTER DATABASE database_name CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
            ALTER TABLE table_name CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
            ALTER TABLE table_name CHANGE column_name column_name VARCHAR(140) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL;
        """
        select_new_user_sql = """
            SELECT DISTINCT weibo_url FROM weibo w 
            WHERE 1=1   -- Weibo 还没有爬过comment 的列表
            AND createdate  > date_sub(NOW(), INTERVAL '7' DAY)
            AND  not exists(
            SELECT * FROM weibocomment WHERE weibo_url = w.weibo_url)
            AND cast(weibo_thumb_up_num AS signed) > 20
            ORDER BY w.createdate DESC
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_new_user_sql)
        for res in cursor.fetchall():
            yield res[0]
