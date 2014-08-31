import os
import time
import facebook
from logbook import Logger

_script_ = (os.path.basename(__file__)
            if __name__ == "__main__"
            else __name__)
logging = Logger(_script_)

API_VERSION = 'v2.0'

class StreamListener(object):

    def __init__(self):
        logging.debug('listener init')
    def on_connect(self):
        pass
    def on_data(self, raw_data):
        logging.error(raw_data.keys())
        # test for print out.

class Stream(object):
    fb_api = None
    def __init__(self, auth, listener, **options):
        self.auth = auth
        self.listener = listener
        self.running = False
        self.client_id = options.get('client_id', None)
        self.client_secret = options.get("client_secret", None)
        logging.info(u"client id/secret is: {0}/{1}", self.client_id, self.client_secret)
        if self.client_id and self.client_secret:
            self.expired = 3600
        else:
            self.expired = None
        #self.timeout = options.get('timeout')
        #self.retry_count = options.get("retry_count")
        # values according to https://dev.twitter.com/docs/streaming-apis/connecting#Reconnecting
        self.retry_time_start = options.get("retry_time", 60)
        #self.retry_420_start = options.get("retry_420", 60.0)
        self.retry_time_cap = options.get("retry_time_cap", 320.0)
        self.snooze_time_step = options.get("snooze_time", 0.25)
        self.snooze_time_cap = options.get("snooze_time_cap", 16)
        self.buffer_size = options.get("buffer_size",  1500)
    
        #self.api = API()
        #self.session = requests.Session()
        #self.session.headers = options.get("headers") or {}
        #self.session.params = None
        self.body = None
        self.retry_time = self.retry_time_start
        self.snooze_time = self.snooze_time_step

    def _run(self):
        # Authenticate
        args = { 'since' : self.start_time }
        resp = None
        exception = None
        while self.running:
            try:
            # get fb.request feeds + token
            # sleep 5mins
                resp = self.fb_api.request(self.url, args)
                self.listener.on_connect()
                #logging.error(u'running, if on_data, retry in ' + `self.retry_time` )
                self._read_loop(resp['data'])
                if self.expired == None:
                    pass
                elif self.expired >0:
                    self.expired -= self.retry_time
                else:
                    self.extend_token()
                time.sleep(self.retry_time)
                start_time = int( time.time() )
            except Exception as e:
                # any exception is fatal, so kill loop.
                logging.notice( u"request error as {0}, retry later.", e )
                self.expired -= 30
                time.sleep(30)
                continue

        # clean up
        self.running = False

        if exception:
            # call a hanlder first so that the exception can be logged.
            #self.listener.on_exception(exception)
            raise

    def _data(self, data):
        if self.listener.on_data(data) is False:
            self.running = False

    def _read_loop(self, resp):
        # prase feed in feeds.
        while resp:
                feed = resp.pop()
                self._data(feed)


    def _start(self, async):
        self.running = True
        if async:
            self._thread = Thread(target=self._run)
            self.thread.start()
        else:
            self._run()

    def on_closed(self, resp):
        #
        pass
    def userstream(self,**args):
        pass

    def filter(self,follow=None, async=False, start_time=None):
        if self.running:
            pass  # *** there should raise ERROR like already connected. *** #
        self.url = '/%s/me/home' % API_VERSION
        self.fb_api = facebook.GraphAPI(access_token = self.auth)
        if start_time:
            self.start_time = start_time
        else:
            self.start_time = int( time.time() )
        logging.notice(u"start_time is {0}".format( time.strftime("%Y/%m/%d %H:%M:%S %p",time.localtime(int(start_time)) )))
        if self.expired:
            self.extend_token()
        self._start(async)

    def extend_token(self):
        try:
            new_token = self.fb_api.extend_access_token(self.client_id, self.client_secret)
            self.fb_api.access_token = new_token['access_token']
            self.expired = int(new_token['expires']) - 86400
            logging.notice(u'extend_token success, expire in {0}'.format(self.expired))
        except Exception as e:
            logging.debug(u'extend_token error:{exception}',exception=e)

    def disconnect(self):
        if self.running is False:
            return
        self.running = False

    #firehose
    #retweet
    #sample
    #sitestream
