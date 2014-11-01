#!/usr/bin/env python
# encoding: utf-8
"""
feeds-checker.py

Created by lanfon72 a.k.a lanf0n, 貓橘毛 on 2014-08-18.
"""

import sys
import os
import time
#import mimetypes
import argparse
import MySQLdb
import anyjson
#import smtplib
import signal
import pytz
#from email.mime.text import MIMEText
import datetime

import socket
# disable buffering
socket._fileobject.default_bufsize = 0

import httplib
#httplib.HTTPConnection.debuglevel = 1

#import urllib2
import requests
import facebook
import logbook
import tweetsclient
import politwoops
from bs4 import BeautifulSoup

_script_ = (os.path.basename(__file__)
            if __name__ == "__main__"
            else __name__)
log = logbook.Logger(_script_)

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

class FeedsChecker(object):
    def __init__(self, heart):
        self.heart = heart
        self.get_config()

    def init_database(self):
        log.debug("Making DB connection")
        self.database = MySQLdb.connect(
            host=self.config.get('database', 'host'),
            port=int(self.config.get('database', 'port')),
            db=self.config.get('database', 'database'),
            user=self.config.get('database', 'username'),
            passwd=self.config.get('database', 'password'),
            charset="utf8",
            use_unicode=True
        )
        self.database.autocommit(True) # needed if you're using InnoDB
        self.database.cursor().execute('SET NAMES UTF8')

    def init_beanstalk(self):
        feeds_tube = self.config.get('beanstalk', 'tweets_tube')
        log.info("Initiating beanstalk connection. Queueing tweets to {use}...", use=feeds_tube)
        self.beanstalk = politwoops.utils.beanstalk(host=self.config.get('beanstalk', 'host'),
                                                    port=int(self.config.get('beanstalk', 'port')),
                                                    watch=None,
                                                    use=feeds_tube)

    def init_facebook_api(self):
        access_token = self.config.get('facebook-client', 'facebook_token')
        self.fb_api = facebook.GraphAPI(access_token)
        log.info(u"Connecting to facebook api...")

    def _database_keepalive(self):
        cur = self.database.cursor()
        cur.execute("""SELECT id FROM feeds LIMIT 1""")
        cur.fetchone()
        cur.close()
        log.info("Executed database connection keepalive query.")

    def get_config(self):
        log.debug("Reading config ...")
        self.config = tweetsclient.Config().get()

    def get_users(self):
        cursor = self.database.cursor()
        cursor.execute("SELECT `facebook_id`, `user_name` FROM `politicians`")
        politicians = { t[0]:t[1] for t in cursor.fetchall() }
        log.info(u"Found politicians:{politicians}",politicians=politicians)

        cursor.execute("SELECT `facebook_id`, `ignored` FROM `normal_users`")
        normal_users = { t[0]:t[1] for t in cursor.fetchall() }
        log.info(u"Found normal_users:{len}",len=len(normal_users))
        return politicians, normal_users

    def run(self):
        self.init_database()
        self.init_beanstalk()
        self.init_facebook_api()

        while True:
            time.sleep(0.2)
            if self.heart.beat():
                self._database_keepalive()
            if self.check_users():
                time.sleep(10) # sleep a I/O tick.
                self.check_tmp_feeds()
            time.sleep(10)
            self.check_feeds()
            time.sleep(200)

    def check_users(self):
        politicians, users = self.get_users()
        cursor = self.database.cursor()
        refresh = {'refresh':False }
        # whether users in politicians.
        for info in set(politicians) & set(users):
            cursor.execute("DELETE FROM `normal_users` WHERE `facebook_id` = %s", info)
            log.notice(u"delete duplicate id {0} in users", info)
            refresh['refresh'] = True
        # insert should not ignored user into politicians.
        cursor.execute("SELECT `facebook_id`, `user_name` FROM `normal_users` WHERE `ignored` = -1")
        for info in cursor.fetchall():
            cursor.execute("INSERT INTO `politicians` (`facebook_id`, `user_name`) VALUES (%s, %s)", (info[0], info[1]))
            cursor.execute("DELETE FROM `normal_users` WHERE `facebook_id` = %s", info[0])
            _msg = u"歡迎新的飼料提供者 {usr} \nFB連結:http://www.facebook.com/{f_id}".format(usr=info[0],f_id=info[1])
            self.fb_api.put_wall_post(_msg.encode('utf-8'))
            log.notice(u"Let user {0} into politicians.", info[1])
            refresh['refresh'] = True
        # notice worker should refresh user list.
        self.beanstalk.put(anyjson.serialize(refresh))
        if refresh['refresh']:
            log.notice(u"Queued refresh feed.")
        return refresh

    def check_tmp_feeds(self):
        politicians, users = self.get_users()
        cursor = self.database.cursor()
        cursor.execute("SELECT `user_id`, `feed`, `id` from `tmp_feeds`")
        # insert new politican's feed from tmp_feeds.
        for info in cursor.fetchall():
            if info[0] not in users:
                self.beanstalk.put(info[1].encode('utf8'))
                cursor.execute("""DELETE FROM `tmp_feeds` WHERE `id` = %s""", info[2])
                log.notice(u"Queued {0}'s tmp_feed.",info[0])                

    def check_feeds(self):
        cursor = self.database.cursor()
        chk_day = datetime.datetime.today() - datetime.timedelta(days=2)
        #cursor.execute("SELECT `id`, `url`, `feed` FROM `feeds` WHERE `deleted` =0 and politician_id=50")
        cursor.execute("SELECT `id`, `url`, `feed` FROM `feeds` WHERE `deleted` = 0 and created>%s", chk_day.strftime("%Y/%m/%d"))
        feeds = cursor.fetchall()
        log.notice(u"counts:{0}", len(feeds))
        for data in feeds:
            time.sleep(0.1)  #delay a tick.
            try:
                # feed exist, put into for work.
                feed = self.fb_api.get_object(data[0])
                #log.notice(u"from {0}", feed['from']['name'])
                self.beanstalk.put(anyjson.serialize(feed))
            except Exception as e:
                # can't access feed by api, try through url.
                cursor.execute("""UPDATE `feeds` SET `unaccessable`=1 WHERE id = %s""",data[0])
                raw_feed = anyjson.deserialize(data[2])
                isactivity = True if u"likes a" in raw_feed.get('story','') or u"like a" in raw_feed.get('story','') or u"commented on" in raw_feed.get('story','') or u"a activity" in data[1] else False
                log.notice(u"raw_story:{0}, isactivity:{1}, raw_url:{2}", raw_feed.get('story',''), isactivity, data[1])
                if not isactivity:
                    html = requests.get(data[1], allow_redirects=True)
                    log.notice("status code:{0}", html.status_code)
                    if html.status_code == requests.codes.not_found:
                        title = BeautifulSoup(html.text).title.string
                        time.sleep(0.5) #sleep a I/O tick.
                        log.notice(u"Title:{0}, url:{1}", title, data[1])
                        if u"找不到網頁" in title or u"Page Not Found" in title: # be deleted.
                            self.handle_deletion(data[0])

    def handle_deletion(self, feed_id):
        cursor = self.database.cursor()
        cursor.execute("""UPDATE `feeds` SET `deleted`=1 WHERE id = %s""", feed_id)
        cursor.execute("""REPLACE INTO `deleted_feeds` SELECT * FROM `feeds` WHERE id = %s AND `content` IS NOT NULL""", feed_id)
        cursor.execute("""SELECT `user_name`, `url`, `content` FROM `deleted_feeds` WHERE  `id` = %s""", feed_id)
        del_feed = cursor.fetchone()
        link = "https://s3-ap-southeast-1.amazonaws.com/twpolitwoops/feed-imgs/" + feed_id + "-0.png"
        account = del_feed[1].split('posts')[0]
        msg = u"吃到 {user_name} 刪除的貼文惹~ 潮爽德~~ \n帳號:{acc}\n原網址:{url}\n原文:{content}"
        msg = msg.format(user_name=del_feed[0], url=del_feed[1], content=del_feed[2], acc=account)
        self.fb_api.put_wall_post(msg.encode('utf-8'), {'link':link})
        log.warn(u"capture a deleted feed!!")


def main(args):
    signal.signal(signal.SIGHUP, politwoops.utils.restart_process)

    log_handler = politwoops.utils.configure_log_handler(_script_, args.loglevel, args.output)
    with logbook.NullHandler():
        with log_handler.applicationbound():
            try:
                log.info("Starting feed checker...")
                log.notice(u"Log level {0}".format(log_handler.level_name))

                with politwoops.utils.Heart() as heart:
                    politwoops.utils.start_watchdog_thread(heart)
                    app = FeedsChecker(heart)
                    if args.restart:
                        return politwoops.utils.run_with_restart(app.run)
                    else:
                        try:
                            return app.run()
                        except Exception as e:
                            logbook.error("Unhandled exception of type {exctype}: {exception}",
                                          exctype=type(e),
                                          exception=str(e))
                            if not args.restart:
                                raise

            except KeyboardInterrupt:
                log.notice(u"Killed by CTRL-C")


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description=__doc__)
    args_parser.add_argument('--loglevel', metavar='LEVEL', type=str,
                             help='Logging level (default: notice)',
                             default='notice',
                             choices=('debug', 'info', 'notice', 'warning',
                                      'error', 'critical'))
    args_parser.add_argument('--output', metavar='DEST', type=str,
                             default='-',
                             help='Destination for log output (-, syslog, or filename)')
    args_parser.add_argument('--restart', default=False, action='store_true',
                             help='Restart when an error cannot be handled.')

    args = args_parser.parse_args()
    sys.exit(main(args))
