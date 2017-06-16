

import MySQLdb
import datetime
import tweepy
import json
import pickle
import multiprocessing as mp
import os

data_path = '/mnt/data/Data/datasets/twitter feed/'

curdate = str(datetime.datetime.now().strftime('%Y-%m-%d'))

db = MySQLdb.connect('localhost', 'lucas', '', 'twitter')

new_cursor = db.cursor()

#####################

config = {'oauth_token': '', 
          'oauth_token_secret': '', 
          'key': '', 
          'secret': ''}

raw_queue = mp.Queue()
count = mp.Queue()

#####################



def load_tweets(keywords):

    global raw_queue
    try:
        l = StdOutListener()
        print 'instantiated l'
        auth = tweepy.OAuthHandler(config['key'], config['secret'])
        auth.set_access_token(config['oauth_token'], config['oauth_token_secret'])
        print 'set auth'
        stream = tweepy.Stream(auth, StdOutListener(), timeout = 60)
        stream.filter(track=keywords)

    except:
        print "failed"


#####################

def tweet_processor(raw_queue, count):
    while True:
        c = count.qsize()
        try:
            data = raw_queue.get(True)
            if 'text' in data: 
                cur = db.cursor()
        
                try:
                    raw = pickle.dumps(data).encode("zip").encode("base64").strip()
                except:
                    print 'pickle failed'

                sn = data['user']['screen_name'].encode('utf-8')
                tweet = data['text'].encode('utf-8')
                tweet = tweet.replace('http://', '')
                tweet = tweet.replace('\n', ' ')
                tweet = tweet.replace("'", "")
                tweet = tweet.replace('\\', '')
                timestamp = datetime.datetime.strptime(data['created_at'],'%a %b %d %H:%M:%S +0000 %Y').strftime('%Y-%m-%d %H:%M:%S')
                try:
                    cur.execute("""INSERT INTO tweets (timestamp, user, text, raw_pickle) VALUES ('""" + timestamp +"""', '"""+ sn + """', '"""+ tweet + """' , '""" + raw + """')""")
                    if (c % 1000 == 0):
                        print ""
                        print 'Inserted', c, 'tweets at\t', str(datetime.datetime.now().strftime('%I:%M:%S %p'))
                        db.commit()
                except:
                    # print 'error inserting\n', timestamp,'\t', sn, '\t', tweet
                    print ".",
                    
        except:
            print 'unable to pop from queue'

#####################

# class MyStreamer(TwythonStreamer): 

#     global raw_queue
#     global count

#     def on_success(self, data): 
#         try:
#             raw_queue.put(data)
#             count.put([1])
#         except:
#             print 'unable to enqueue'

#     def on_error(self, status_code, data): 
#         print status_code

class StdOutListener(tweepy.StreamListener):
    global raw_queue
    global count

    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        try:
            raw_queue.put(json.loads(data))

            print ''
            return True
        except:
            print 'fail'

    def on_error(self, status):
        print status


########################


## keywords = ['twitter', 'linux', 'microsoft', 'facebook']
keywords = ['ict', 'twitter', '#tascha', '#ict', 'seattle', 'library', 'libraries''facebook']



process_list = []

# l = StdOutListener()
# auth = tweepy.OAuthHandler(config['key'], config['secret'])
# auth.set_access_token(config['oauth_token'], config['oauth_token_secret'])

# stream = tweepy.Stream(auth, l)


process_list.append(mp.Process(
        target = load_tweets,
        args = ([keywords])))

for i in range(3):
    process_list.append(mp.Process(
            target = tweet_processor,
            args = (raw_queue, count)))

# the_pool = mp.Pool(3, tweet_processor, (raw_queue, count))

print "Starting processes at: %s" % datetime.datetime.now()

for p in process_list:
    p.start()

