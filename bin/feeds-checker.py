#!/usr/bin/env python
# encoding: utf-8
"""
feeds-checker.py

Created by lanfon72 a.k.a lanf0n, 貓橘毛 on 2014-08-18.
"""

import sys
import os
import re
import time
#import mimetypes
import argparse
import MySQLdb
import anyjson
#import smtplib
import signal
import pytz
#from email.mime.text import MIMEText
from datetime import datetime

import socket
# disable buffering
socket._fileobject.default_bufsize = 0

import httplib
httplib.HTTPConnection.debuglevel = 1

#import urllib2
import requests
import facebook
import logbook
import tweetsclient
import politwoops
replace_highpoints = politwoops.utils.replace_highpoints

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
        log.info("Found politicians:{politicians}",politicians=politicians)

        cursor.execute("SELECT `facebook_id`, `ignored` FROM `normal_users`")
        normal_users = { t[0]:t[1] for t in cursor.fetchall() }
        log.info("Found normal_users:{len}",len=len(normal_users))
        return politicians, normal_users

    def run(self):
        self.init_database()
        self.init_beanstalk()
        self.init_facebook_api()

        whie True:
            time.sleep(0.2)
            if self.heart.beat():
                self._database_keepalive()
            if self.check_users():
                self.check_tmp_feeds()
            self.check_feeds()

    def check_users(self):
        politicians, users = self.get_users()
        cursor = self.database.cursor()
        refresh = {'refresh':False }
        # whether users in politicians.
        for info in set(politicians) & set(users):
            cursor.execute("DELETE FROM `normal_users` WHERE `facebook_id` = %s", info)
            log.notice("delete duplicate id {0} in users", info)
            refresh['refresh'] = True
        # insert should not ignored user into politicians.
        cursor.execute("SELECT `facebook_id`, `user_name` FROM `normal_users` WHERE `ignored` = -1")
        for info in cursor.fetchall():
            cursor.execute("INSERT INTO `politicians` (`facebook_id`, `user_name`) VALUES (%s, %s)", (info[0], info[1]))
            cursor.execute("DELETE FROM `normal_users` WHERE `facebook_id` = %s", info[0])
            log.notice("Let user {0} into politicians.", info[1])
            refresh['refresh'] = True
        # notice worker should refresh user list.
        self.beanstalk.put(anyjson.serialize(refresh))
        return refresh['refresh']

    def check_tmp_feeds(self):
        politicians, users = self.get_users()
        cursor = self.database.cursor()
        cursor.execute("SELECT `user_id`, `feed` from `tmp_feeds`")
        # insert new politican's feed from tmp_feeds.
        for info in cursor.fetchall():
            if info[0] not in users:
                self.beanstalk.put(anyjson.serialize(info[1]))

    def check_feeds(self):
        cursor = self.database.cursor()
        cursor.execute("SELECT `id`, `url`, FROM `feeds` WHERE `deleted` = 0")
        for data in cursor.fetchall():
            try:
                # feed exist, put into for work.
                feed = self.fb_api.get_object(data[0])
                self.beanstalk.put(anyjson.serialize(feed))
            except Exception as e:
                # can't access feed by api, try through url.
                cursor.execute("""UPDATE `feed` SET `unaccessable`=1 WHERE id = %s""" % (data[0]))
                html = requests.get(data[1])
                isdelete = re.findall(u'id="pageTitle">(.*)',html.text)
                if "Page Not Found" in isdelete: #is deleted.
                    self.handle_deletion(data[0])

    def handle_deletion(self, feed_id):
        cursor = self.database.cursor()
        cursor.execute("""UPDATE `feeds` SET `deleted`=1 WHERE id = %s""" % (feed_id))
        cursor.execute("""REPLACE INTO `deleted_feeds` SELECT * FROM `feeds` WHERE id=%s AND `content` IS NOT NULL""" % (feed_id))
        log.warn(u"capture a deleted feed!!")


def main(args):
    signal.signal(signal.SIGHUP, politwoops.utils.restart_process)

    log_handler = politwoops.utils.configure_log_handler(_script_, args.loglevel, args.output)
    with logbook.NullHandler():
        with log_handler.applicationbound():
            try:
                log.info("Starting feed checker...")
                log.notice("Log level {0}".format(log_handler.level_name))

                with politwoops.utils.Heart() as heart:
                    politwoops.utils.start_watchdog_thread(heart)
                    app = DeletedTweetsWorker(heart, args.images)
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
                log.notice("Killed by CTRL-C")


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