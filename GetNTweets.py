# -*- coding: utf-8 -*-
"""
@author: jesseclark

Use Tweepy 3.2.0, not 3.3.0
Seems to erro with 3.3.0

"""
#Import the necessary methods from tweepy library
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys


#Variables that contains the user credentials to access Twitter API 
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""

class StdOutListener(StreamListener):

    def __init__(self, api=None,save_name = 'test.txt',tweets_total = 100):
        super(StdOutListener, self).__init__()
        
        # set the ~ total number of tweets to collet
        self.tweets_total = tweets_total        
        # specify the file to write the tweets to        
        self.f = open(save_name, "w")
        
        # init the counter
        self.num_tweets = 0
        

    def on_data(self, data):
        # increment collected tweet counter
        self.num_tweets += 1
        if self.num_tweets <= self.tweets_total:
            print str(self.num_tweets)
            self.f.write( data  )      
            return True
        else:
            self.f.close()
            return False


    def on_error(self, status):
        print 'Error on status', status

    def on_limit(self, status):
        print 'Limit threshold exceeded', status

    def on_timeout(self, status):
        print 'Stream disconnected; continuing...'


def collectTweets(tweets_total, save_name ,location ):

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener(save_name = save_name, tweets_total = tweets_total)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret) 
    stream = Stream(auth, l)
    
    if len(location) != 0:
        stream.filter(locations=[-122.75,36.8,-121.75,37.8],track = [location],languages = ['en'])
    else:
        stream.filter(languages = ['en'])
    
def collectTweetsLoc(tweets_total, save_name ,location ):

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener(save_name = save_name, tweets_total = tweets_total)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret) 
    stream = Stream(auth, l)
    
    stream.filter(locations=location,languages = ['en'])
    
def post_tweet(tweet,to_tweet = 1):
    # use this to post a tweet to attached api acount
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret) 
    
    #if to_tweet != 0:     
    api = tweepy.API(auth)
    api.update_status(status = tweet)        
    print tweet+'\n'
    

if __name__ == '__main__':

    # collect tweets from the fire hose
    # see GetCityTweets.py to see how to collect from location

    # tweets to collect, name and filter key
    tweets_total = 75
    save_name = 'test.txt'
    key = ':)'    
    
    # save name if entered by user
    if len(sys.argv) >= 2:
        save_name = sys.argv[1]
    # total tweets if entered by user
    if len(sys.argv) >= 3:
        tweets_total = int(sys.argv[2])
    # what to filter on (text)
    if len(sys.argv) >= 4:
        key = (sys.argv[3])


    print '\n Collecting tweets ['+str(tweets_total)+'].... \n'
    print '\n '+key    
    print '\n '+save_name
    
    collectTweets(tweets_total , save_name , key)
    