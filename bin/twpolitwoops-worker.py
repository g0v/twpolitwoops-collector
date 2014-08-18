#!/usr/bin/env python
# encoding: utf-8
"""
politwoops-worker.py

Created by Breyten Ernsting on 2010-05-30.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import time
import mimetypes
import argparse
import MySQLdb
import anyjson
import smtplib
import signal
import pytz
import difflib
from email.mime.text import MIMEText
from datetime import datetime

import socket
# disable buffering
socket._fileobject.default_bufsize = 0

import httplib
httplib.HTTPConnection.debuglevel = 1

#import urllib2
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


class DeletedTweetsWorker(object):
    def __init__(self, heart, images):
        self.heart = heart
        self.images = images
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
        tweets_tube = self.config.get('beanstalk', 'tweets_tube')
        screenshot_tube = self.config.get('beanstalk', 'screenshot_tube')

        log.info("Initiating beanstalk connection. Watching {watch}.", watch=tweets_tube)
        if self.images:
            log.info("Queueing screenshots to {use}.", use=screenshot_tube)

        self.beanstalk = politwoops.utils.beanstalk(host=self.config.get('beanstalk', 'host'),
                                                    port=int(self.config.get('beanstalk', 'port')),
                                                    watch=tweets_tube,
                                                    use=screenshot_tube)

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
        q = "SELECT `facebook_id`, `user_name`, `id` FROM `politicians`"
        cursor.execute(q)
        ids = {}
        politicians = {}
        for t in cursor.fetchall():
            ids[t[0]] = t[2]
            politicians[t[0]] = t[1]
        log.info("Found ids:{ids}",ids=ids)
        log.info("Found politicians:{politicians}",politicians=politicians)
        return ids, politicians

    def get_normal_users(self):
        cursor = self.database.cursor()
        q = "SELECT `facebook_id`,`ignored` FROM `normal_users`"
        cursor.execute(q)
        normal_users = { t[0]:t[1] for t in cursor.fetchall() }
        log.info("Found normal_users:{len}",len=len(normal_users))
        return normal_users

    def run(self):
        #minetypes.init()
        self.init_database()
        self.init_beanstalk()
        self.users, self.politicians = self.get_users()
        self.normal_users = self.get_normal_users()

        while True:
            time.sleep(0.2)
            if self.heart.beat():
                self._database_keepalive()
            reserve_timeout = max(self.heart.interval.total_seconds() * 0.1, 2)
            job = self.beanstalk.reserve(timeout=reserve_timeout)
            if job:
                self.handle_feed(job.body)
                job.delete()

    def handle_feed(self, job_body):
        #log.notice(u'handle_feed.')
        #if feed.has_key('delete'):
            #if delete feed's user_id in self.users.keys():
            #    self.handle_deletion(feed)
        #    pass
        #else:
        feed = anyjson.deserialize(job_body)
        if isinstance(feed, unicode):
            feed = anyjson.deserialize(feed)
            print feed.get('from',{}).get('name')

        if feed.has_key('refresh'):
            if feed.get('refresh'):
                self.users, self.politicians = self.get_users()
                self.normal_users = self.get_normal_users()
                log.notice(u"Refresh user and politician list from check notice.")
        elif feed.get('from',{}).get('id') in self.users.keys(): #is a politician
            self.handle_new(feed)
        elif feed.get('from',{}).get('id') in self.normal_users.keys(): #is a normal user
            if not self.normal_users[ feed.get('from',{}).get('id') ]:  #not be ignored.
                self.handle_tmp(feed)
        else:
            cursor = self.database.cursor()
            cursor.execute("""INSERT INTO normal_users (`user_name`, `facebook_id`) VALUES (%s, %s)""",
                            (feed.get('from',{}).get('name'), feed.get('from',{}).get('id')) )
            log.notice(u"add new normal user:{0}({1})", feed.get('from',{}).get('name'), feed.get('from',{}).get('id') )
            self.normal_users = self.get_normal_users()
            self.handle_tmp(feed)

            #if self.images and tweet.has_key('entities'):
            #        # Queue the tweet for screenshots and/or image mirroring
            #        log.notice("Queued tweet {0} for entity archiving.", tweet['id'])
            #        self.beanstalk.put(anyjson.serialize(tweet))

    def handle_deletion(self, tweet):
        
        #log.notice("Deleted tweet {0}", tweet['delete']['status']['id'])
        #cursor = self.database.cursor()
        #cursor.execute("""SELECT COUNT(*) FROM `tweets` WHERE `id` = %s""", (tweet['delete']['status']['id'],))
        #num_previous = cursor.fetchone()[0]
        #if num_previous > 0:
        #    cursor.execute("""UPDATE `tweets` SET `modified` = NOW(), `deleted` = 1 WHERE id = %s""", (tweet['delete']['status']['id'],))
        #else:
        #    cursor.execute("""REPLACE INTO `tweets` (`id`, `deleted`, `modified`, `created`) VALUES(%s, 1, NOW(), NOW())""", (tweet['delete']['status']['id']))
        #self.copy_tweet_to_deleted_table(tweet['delete']['status']['id'])

        #cursor.execute("""SELECT * FROM `tweets` WHERE `id` = %s""", (tweet['delete']['status']['id'],))
        #ref_tweet = cursor.fetchone()
        #self.send_alert(ref_tweet[1], ref_tweet[4], ref_tweet[2])
        pass

    def handle_new(self, feed):
        self.handle_possible_rename(feed); #check user whether rename.
        cursor = self.database.cursor()
        cursor.execute("""SELECT COUNT(*), `deleted`, `content`, `modified`, `edited_list` FROM `feeds` WHERE `id` = %s""",(feed['id']))
        
        info = cursor.fetchone()
        num_previous = info[0]
        if info[1] is not None:
            was_deleted = (int(info[1]) == 1)
        else:
            was_deleted = False

        if num_previous > 0: #feed exist.
            if feed.has_key('message'):
                if list( difflib.context_diff(info[2], feed.get('message')) ):
                    msg = { "message":info[2], "updated_time":str(info[3]) }
                    if "null" in str(info[-1]):
                        edited_list = list()
                        edited_list.append(msg)
                    else:
                        edited_list = anyjson.deserialize(info[-1])
                        edited_list.append(msg)
                    cursor.execute("""UPDATE `feeds` SET `user_name`=%s, `politician_id`=%s,`content`=%s, `modified`=%s, `edited_list`=%s WHERE id=%s""",
                                (feed['from']['name'],
                                 self.users[feed['from']['id']],
                                 feed['message'],
                                 feed.get('updated_time').replace('+0000',''),
                                 anyjson.serialize(edited_list),
                                 feed['id']))
                    log.notice( u"Updated {0}'s feed {0}", feed.get('from',{}).get('name'), feed.get('id') )
            else:
                log.info(u"{0}'s feed hasn't message key.", feed['from']['name'] )
            
        else:
            url=""
            if feed.has_key('actions'):
                url = feed.get('actions')[0].get('link')
            else:
                url = "is a activity."
            cursor.execute("""INSERT INTO `feeds` (`id`, `user_name`, `politician_id`, `content`, `created`, `modified`, `feed`, `feed_type`, url, edited_list) 
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (feed.get('id'),
                            feed.get('from',{}).get('name'),
                            self.users[feed['from']['id']], 
                            feed.get('message',''),
                            feed.get('created_time').replace('+0000',''),
                            feed.get('updated_time').replace('+0000',''),
                            anyjson.serialize(feed),
                            feed.get('type'),
                            url,
                            "[]") )

            log.notice( u"Inserted {1}'s new feed {0}", feed.get('id'), feed.get('from',{}).get('name') )


        if was_deleted:
            log.warn("feed deleted {0} before it came!", feed.get('id'))
            #self.copy_tweet_to_deleted_table(feed['id'])

    def handle_tmp(self, feed):
        cursor = self.database.cursor()
        cursor.execute("""SELECT COUNT(*), `id` FROM `tmp_feeds` WHERE `id` = %s""",(feed['id']))
        info = cursor.fetchone()
        if int(info[0])==0: #not exist.
            cursor.execute("""INSERT INTO `tmp_feeds` (`id`,`user_id`,`user_name`,`content`, `created`, `modified`, `feed`, `feed_type`) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (feed.get('id'),
                            feed.get('from',{}).get('id'),
                            feed.get('from',{}).get('name'),
                            feed.get('message',''),
                            feed.get('created_time').replace('+0000',''),
                            feed.get('updated_time').replace('+0000',''),
                            anyjson.serialize(feed),
                            feed.get('type') ) )
            log.info(u"normal user {0}'s feed {1} insert into tmp.", feed.get('from',{}).get('name'), feed.get('id'))

    def copy_tweet_to_deleted_table(self, feed_id):
        cursor = self.database.cursor()
        cursor.execute("""REPLACE INTO `deleted_tweets` SELECT * FROM `tweets` WHERE `id` = %s AND `content` IS NOT NULL""" % (tweet_id))

    def handle_possible_rename(self, feed):
        """ check user whether change name. """
        feed_user_name = feed['from']['name']
        feed_user_id = feed['from']['id']
        current_user_name = self.politicians[feed_user_id]
        if current_user_name != feed_user_name:
            self.politicians[feed_user_id] = feed_user_name
            cursor = self.database.coursor()
            cursor.execute("""UPDATE `politicians` SET `user_name` = %s WHERE `id` = %s""", (feed_user_name, self.users[feed_user_id]))

    def send_alert(self, username, created, text):
        """
        if username and self.config.has_section('moderation-alerts'):
            host = self.config.get('moderation-alerts', 'mail_host')
            port = self.config.get('moderation-alerts', 'mail_port')
            user = self.config.get('moderation-alerts', 'mail_username')
            password = self.config.get('moderation-alerts', 'mail_password')
            recipient = self.config.get('moderation-alerts', 'twoops_recipient')
            sender = self.config.get('moderation-alerts', 'sender')

            if not text:
                #in case text is None from a deleted but not originally captured deleted tweet
                text = ''
            text += "\n\nModerate this deletion here: http://politwoops.sunlightfoundation.com/admin/review\n\nEmail the moderation group if you have questions or would like a second opinion at politwoops-moderation@sunlightfoundation.com"

            nowtime = datetime.now()
            diff = nowtime - created
            diffstr = ''
            if diff.days != 0:
                diffstr += '%s days' % diff.days
            else:
                if diff.seconds > 86400:
                    diffstr += "%s days" % (diff.seconds / 86400 )
                elif diff.seconds > 3600:
                    diffstr += "%s hours" % (diff.seconds / 3600)
                elif diff.seconds > 60:
                    diffstr += "%s minutes" % (diff.seconds / 60)
                else:
                    diffstr += "%s seconds" % diff.seconds

            nowtime = pytz.timezone('UTC').localize(nowtime)
            nowtime = nowtime.astimezone(pytz.timezone('US/Eastern'))

            smtp = smtplib.SMTP(host, port)
            smtp.login(user, password)
            msg = MIMEText(text.encode('UTF-8'), 'plain', 'UTF-8')
            msg['Subject'] = 'Politwoop! @%s -- deleted on %s after %s' % (username, nowtime.strftime('%m-%d-%Y %I:%M %p'), diffstr)
            msg['From'] = sender
            msg['To'] = recipient
            smtp.sendmail(sender, recipient, msg.as_string())
        """

def main(args):
    #configuration
    signal.signal(signal.SIGHUP, politwoops.utils.restart_process)

    log_handler = politwoops.utils.configure_log_handler(_script_, args.loglevel, args.output)
    with logbook.NullHandler():
        with log_handler.applicationbound():
            try:
                log.info("Starting Politwoops worker...")
                log.notice("Log level {0}".format(log_handler.level_name))
                if args.images:
                    log.notice("Screenshot support enabled.")

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
    #args settings
    args_parser = argparse.ArgumentParser(description=__doc__)
    args_parser.add_argument('--loglevel', metavar='LEVEL', type=str,
                             help='Logging level (default: notice)',
                             default='notice',
                             choices=('debug', 'info', 'notice', 'warning',
                                      'error', 'critical'))
    args_parser.add_argument('--output', metavar='DEST', type=str,
                             default='-',
                             help='Destination for log output (-, syslog, or filename)')
    args_parser.add_argument('--images', default=False, action='store_true',
                             help='Whether to screenshot links or mirror images linked in tweets.')
    args_parser.add_argument('--restart', default=False, action='store_true',
                             help='Restart when an error cannot be handled.')

    args = args_parser.parse_args()
    sys.exit(main(args))
