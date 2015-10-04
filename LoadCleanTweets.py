# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 14:18:24 2015

@author: jesseclark

Class and functions for reading and processing tweets
assumes json file format of the tweets

The tweets class is good for reading and processing

There are also quite a few helper functions for processing the 
tweets and strings in general

"""
from nltk.corpus import stopwords
import json
import random
import pandas as pd
import re
import numpy as np
from collections import Counter
import string
import operator
import cPickle as pickle
from datetime import datetime as dt

# define a tweets class
class LoadCleanTweets:
    
    def __init__(self):
        # default cleaning option    
        self.rem_url_tags = True 
        # can also classify the tweets, point to the path
        self.classify_path = './Classify'
        # randomize the tweets before saving?
        self.shuffle_before_saving = False
        
        # a coord map for state coords to map the geo location to a state
        try:
            self.state_coords = pd.DataFrame.from_csv('state_latlon.csv')
        except:
            self.state_coords = []
            print("Could not find state coords file....")
            
            
    def load_tweets(self, file_path = ""):
        # load the tweets, specify a path
        tweets_file = open(file_path, "r")
        
        self.tweets = []
        self.file_path = file_path        
        
        # read in the tweets from the json
        for line in tweets_file:

            # use try for the /n lines 
            try:
                tweet = json.loads(line)
                self.tweets.append(tweet)
            except:
                continue
            
    def save_tweets(self, file_path = ""):
        # save the tweets, specify a save path
        # saves from a df frame - must run extract_to_data_Frame() first
        save_list_to_text(self.tweets_df['text'], file_path)

    def extract_to_data_frame(self):
        # take the loaded tweets and put relevant fields into
        # a pandas frame        
        
        # a flag to check if classification has ben done on the tweets
        self.get_sentiment_flag = False
        
        # make a pandas data frame and do some filtering/text 
        self.tweets_df = pd.DataFrame()
        
        # need latest version of pandas otherwise you get an attribute error
        # get the tweet text and lang
        self.tweets_df['text'] = [ get_field_from_tweet(tweet,'text') for tweet in self.tweets]
        self.tweets_df['lang'] = [ get_field_from_tweet(tweet,'lang') for tweet in self.tweets]
        self.tweets_df['retweeted'] = [ get_field_from_tweet(tweet,'retweeted') for tweet in self.tweets]
        
        # id and place
        self.tweets_df['id'] = [ get_field_from_tweet(tweet,'id') for tweet in self.tweets]
        self.tweets_df['place'] = [ get_coords_from_tweet(tweet) for tweet in self.tweets]
        
        # get avvg gps and map to the ag state (should be good enough for me)
        self.tweets_df['state'] = [ get_state_from_latlong(latlong,self.state_coords) for latlong in self.tweets_df['place']]
        
        # store UTC time and as a time object for filtering
        self.tweets_df['created_at'] = [ get_field_from_tweet(tweet,'created_at') for tweet in self.tweets ]        
        self.tweets_df['time_obj'] = [get_time_from_tweet(tweet) for tweet in self.tweets]
        
        # store the center time seperately, (don't have to get the length later)
        self.time_center =  self.tweets_df['created_at'][round(len(self.tweets_df)/2)]       
        
        # now do some processing, remove rt, @ hhtp & etc
        if self.rem_url_tags:
            self.tweets_df['text'] = [ remove_url_no_tags(tweet) for tweet in self.tweets_df['text']]
        
        # store the hashtags (if present)
        self.tweets_df['hashtag'] = [ get_hashtag_from_tweet(tweet) for tweet in self.tweets]
        
        # get number of smiley and frowney faces
        self.tweets_df['smiley'] = [ word_in_text(':)',tweet) for tweet in self.tweets_df['text']]
        self.tweets_df['frowney'] = [ word_in_text(':(',tweet) for tweet in self.tweets_df['text']]
        
        # use number of :) and (: to get training label
        self.tweets_df['sent_val'] = self.tweets_df['smiley'] - self.tweets_df['frowney'] 
        self.tweets_df['sent_lab'] = [ number_to_sent_label(sent_val) for sent_val in self.tweets_df['sent_val'] ] 
        
    def add_hashtag_to_text(self):
        # puts the # add the end of the text.  only use this if it was stripped 
        # using one of the cleaning functions        
        self.tweets_df['text'] = [add_hashtag(text,hashtag) for text,hashtag in zip(self.tweets_df['text'],self.tweets_df['hashtag'])]
        
    def add_sent_to_text(self):
        # can tag the tweets with the calculated sent. e.g. blah blah #blah pos
        self.tweets_df['text'] = [add_text(text,' !'+sent) for text,sent in zip(self.tweets_df['text'],self.tweets_df['sent_lab_cl'])]
    
    
    def utf_encode(self):
        # convert to unicode
        self.tweets_df['text'] = [text.strip().encode('utf-8') for text in self.tweets_df['text']]
     
    def get_sentiment(self, classifier = None):
        # can do classification of tweets by passing a classifier object
        # assumes the predict is the function call
        # use sci-kit piplein to make a classifier then it does all the 
        # pre-processing (tf-idf etc) for you
    
        # replace labels with external classifier if present
        if not classifier == None:
            print "Using loaded classifier..."
            self.tweets_df['sent_lab_cl'] = classifier.predict(self.tweets_df['text'])
                
        # flag to indicate if sentiment has been calculated
        self.get_sentiment_flag = True

        # get the smiley frowney values        
        self.smiley_total = self.tweets_df['smiley'].sum()
        self.frowney_total = self.tweets_df['frowney'].sum()

        # set default
        self.sent_cl_totals = pd.DataFrame()
        
        # get clasiified totals        
        self.sent_cl_totals = self.tweets_df['sent_lab_cl'].value_counts()

        # add in the total
        self.sent_cl_totals['total'] = self.sent_cl_totals.sum()
        
        # make a dictionary with results, easy for tweeting the string
        self.emoticon_sent_dic = {'Positive :)':self.smiley_total,'Negative :(':self.frowney_total}
 
        # check for non-zero (can be a problem if total tweets is very low ~10)
        keys = ['pos','neg','net']
        for key in keys:
            if key not in self.sent_cl_totals: self.sent_cl_totals[key] = 0
        
        # this is for compatability with previous tweet bot
        self.nltk_sent_dic = {'Positive :)':self.sent_cl_totals['pos'],'Negative :(':self.sent_cl_totals['neg']}
        
        # return overall sentiment for external use
        self.overall_sentiment = max(self.nltk_sent_dic.iteritems(), key=operator.itemgetter(1))[0]        
        
        
    def remove_punctuation_frame(self):
        # removes the punctuation from the tweets
        table = string.maketrans("","")
        self.tweets_df['text'] = [ remove_punctuation(tweet,table=table) for tweet in self.tweets_df['text']]
        self.punct_removed = True

    def remove_punctuation_partial_frame(self):
        # removes the punctuation from the tweets but is not as agressive as 
        # remove_punctuation_frame (leaves in !, # ?)
        table = string.maketrans("","")
        self.tweets_df['text'] = [ remove_punctuation_partial(tweet,table=table) for tweet in self.tweets_df['text']]
        self.punct_removed = True

    def add_space_partial_frame(self):
        # adds a space to ?, ! a) etc so that words and symbols are seperated
        self.tweets_df['text'] = [ add_space_symbol(tweet) for tweet in self.tweets_df['text']]
        
    def make_lower_frame(self):
        # makes the tweets lower case
        self.tweets_df['text'] = [ tweet.lower() for tweet in self.tweets_df['text']]

    def get_tweet_length(self):
        # retruns the tweet length
        self.tweets_df['text_len'] = self.tweets_df['text'].str.len()
        
    def remove_retweets_neutral(self, remove_retweets = True, remove_neutral = False):
        # removes retweets and/or neutral sentiment ones (used for auto labelling)
        # remove rewteets 
        if remove_retweets:
            self.tweets_df = mask(self.tweets_df, 'retweeted', False)
        
        # remove neutral, useful for training
        if remove_neutral:
            self.tweets_df = not_mask(self.tweets_df, 'sent_val', 0)

    def get_freq_anal(self, nn = 100):
        # get the frequency of words from the tweets
    
        # check if sentiment, if not then get it        
        if not self.get_sentiment_flag:
            self.get_sentiment()
            
        # remove stop words,  add more if you want (just append to list)
        cachedStopWords = stopwords.words("english")
        
        # get only the pos ones for now        
        temp= mask(self.tweets_df, 'sent_lab_cl', 'pos')        
        self.all_tweets_pos = ' '.join(temp['text'])
        self.all_tweets_pos= ' '.join([word for word in self.all_tweets_pos.split() if word not in cachedStopWords])
        self.word_frequency_pos = Counter(self.all_tweets_pos.split()).most_common(nn)
        
        # do negative
        temp= mask(self.tweets_df, 'sent_lab_cl', 'neg')        
        self.all_tweets_neg = ' '.join(temp['text'])
        self.all_tweets_neg = ' '.join([word for word in self.all_tweets_neg.split() if word not in cachedStopWords])
        self.word_frequency_neg = Counter(self.all_tweets_neg.split()).most_common(nn)
        
        # do neutral
        temp= mask(self.tweets_df, 'sent_lab_cl', 'net')        
        self.all_tweets_net = ' '.join(temp['text'])
        self.all_tweets_net = ' '.join([word for word in self.all_tweets_net.split() if word not in cachedStopWords])
        self.word_frequency_net = Counter(self.all_tweets_net.split()).most_common(nn)
        
        
    def save_as_classify_set(self, file_path = "./Classify.txt"):
        # save the current tweets into a classification set, if using :) as an auto-label  
        csv_string = tweet_to_csv_string(self.tweets_df,shuffle_before_saving = True)
        save_list_to_text(csv_string, file_path)

        
    def main(self):
        # an eample pipline for the tweets        
        # executes the main functions in order for anlysis
        self.extract_to_data_frame()
        self.remove_retweets_neutral()
        self.make_lower_frame()
        self.remove_punctuation_frame()
        self.get_freq_anal()
        
        
# some helper functions for text/string manip
    
# this os for filtering the data frame
def mask(data_frame, key, value):
    #if not_equal:
    return data_frame[data_frame[key] == value]

def not_mask(data_frame, key, value):
    #if not_equal:
    return data_frame[data_frame[key] != value]
 
def tweet_to_csv_string(data_frame,shuffle_before_saving = False):
    # pass through the pandas frame to make a csv
    # do it manually as it needs to be a specific format for the classifier
    csv_str = []
    
    if shuffle_before_saving:
        data_frame = shuffle_data_frame(data_frame)

    data_frame = data_frame.reset_index()
    
    for ii in range(0,len(data_frame)):    
        tw_text = remove_mult_words([':)',':(',','],data_frame['text'][ii].lower())        
        o_str = tw_text+','+data_frame['sent_lab'][ii]+'\n'  
        csv_str.append(o_str)

    return csv_str   
    
   
def tweet_to_json_string(data_frame,shuffle_before_saving = False):
    # pass through the pandas frame to make a json
    # do it manually as it needs to be a specific format for the classifier
    json_str = []
    
    if shuffle_before_saving:
        data_frame = shuffle_data_frame(data_frame)

    data_frame = data_frame.reset_index()
    
    for ii in range(0,len(data_frame)):    
        tw_text = remove_mult_words([':)',':('],data_frame['text'][ii].lower())        
        o_str = '{"text": "'+tw_text+'", "label": "'+data_frame['sent_lab'][ii]+'"}' 

        if ii < len(data_frame) - 1:
            o_str = o_str+','
            
        o_str = o_str + '\n'
        
        json_str.append(o_str)

    return json_str   

def save_list_to_text(save_list, file_path = "./Classify/test.txt"):
    # save as a simple text file with new line for reading into R
    counter = 0
        
    with open(file_path, 'w') as thefile:        
        for item in save_list:
            thefile.write(item+' \n')
    
    with open(file_path, "a") as myfile:
        myfile.write("\n")


def loadClassifier(file_path = './Classify/cl.pkl'):
    # load another classifier
    file=open(file_path,'rb')
    cl_load = pickle.load(file)
    return cl_load

def number_to_sent_label(number):
    # given a polarity, make a sentament label
    sent = 'net'    
    if number > 0:
        sent = 'pos'
    elif number < 0:
        sent = 'neg'
    return sent

def shuffle_data_frame(data_frame):  
    # shuffle a data fraeme randomly
    return data_frame.iloc[np.random.permutation(len(data_frame))]

def get_time_from_tweet(tweet):
    # convert to datetime object so we can sort based on time
    t_format = "%b %d %H:%M:%S %Y"
    try:
        tweet_time = dt.strptime(tweet['created_at'][4:19]+tweet['created_at'][25:],t_format)
    except:
        tweet_time = ''
        
    return tweet_time

def get_hashtag_from_tweet(tweet):
    # get a field from a tweet and put a NA if it does not exist
    # used in list comprehension
    try:
        text = tweet["entities"]["hashtags"][0]["text"]
    except:
        text = ''
    return text

def get_coords_from_tweet(tweet):
    # get a field from a tweet and put a NA if it does not exist
    try:
        text = tweet['place']['bounding_box']['coordinates']
    except:
        text = ''
    return text

def get_field_from_tweet(tweet,field = 'text'):
    # get a field from a tweet and put a NA if it does not exist
    try:
        text = tweet[field]
    except:
        text = ''
    return text

def extract_hashtag(tweet):
    # extract a hashtag from within
    hashtags = " ".join([word for word in tweet.split()
                                    if '#' in word]) 
    return hashtags
    

def remove_url_no_tags(tweet):
    # remove urls, tags etc and also make ascii
    no_urls_no_tags = " ".join([word for word in tweet.split()
                            if 'http' not in word
                            and not word.startswith('@') 
                            and '@' not in word
                            and '&' not in word 
                            and word != 'RT'])                               
    
    return no_urls_no_tags.encode("ascii","ignore")

def remove_punctuation_partial(tweet,table = string.maketrans("","")):
    # not as agressive punct removal
    
    # list to remove
    rem_list = '"/\|}{[]@^*+-.,`")(:;'+"'" # ':!)(;#$%'
        
    for ch in rem_list:
        tweet = str.replace(tweet,ch,'')
        
    # remove double spaces within tweet
    tweet = re.sub(' +',' ',tweet)    
    
    return tweet

def add_space_symbol(tweet):
    # add a spce between ): ! ? etc so they are their own symbols - better for classification
    rem_list = ['\'','"','/'," \ ",'|','}','{','[',']','@','^','*','+','-','.','`','!','?',"'",'='] 
    
    for ch in rem_list:
        tweet = str.replace(tweet,ch.strip(),' '+ch.strip()+' ')
    
    return tweet

    
def remove_punctuation(tweet,table = string.maketrans("","")):
    # remove urls, tags etc and also make ascii
    return tweet.translate(table, string.punctuation)
    
def remove_single_word(word_str,text):
     # removes words from a string
     temp_str = " ".join([word for word in text.split() if word_str not in word])
     return temp_str

def remove_mult_words(word_strs,text):
    # removes a list of words from a string
    for word_str in word_strs:
        text = remove_single_word(word_str,text)
    
    return text
    
def add_hashtag(tweet,hashtag):
    if len(hashtag) > 0:
        tweet+=' #'+hashtag.lower()
    return tweet

def add_text(tweet,text):
    if len(text) > 0:
        tweet+=text
    return tweet

def word_instances_in_text(word_str,text):
    # how many times is a word present?
    temp_str = "".join(word for word in text.split() if word_str in word)
    
    return len(temp_str)/len(word_str)
    
    
def word_in_text(word_str,text,return_words = False):
    # extract a word from a tweet
    temp_str = "".join(word for word in text.split() if word_str in word)
    
    if not return_words:
        return len(temp_str)/len(word_str)
    return temp_str
    
def randomize_tweets_file(file_path = ""):
        with open(file_path,'r') as source:
            data = [ (random.random(), line) for line in source ]
        data.sort()
        with open(file_path,'w') as target:
            for _, line in data:
                target.write( line )
        
def remove_other(tweet):
    # remove urls, tags etc and also make ascii
    no_urls_no_tags = " ".join([word for word in tweet.split()
                            if 'xe' not in word
                            and 'xf' not in word
                            and 'x8' not in word 
                            and 'xc' not in word                                        
                            ])                               
    
    return no_urls_no_tags.encode("ascii","ignore")

def get_state_from_latlong(latlong,state_coords = []):
    # take the box from the lat long tweet
    # and map to a state
    # use a try as some places are empty (seems to be very few)
    try:
        
        latlong = [[lat,lng] for (lat,lng) in latlong[0]]

        avg_gps = [np.array(latlong)[:,0].mean(),np.array(latlong)[:,1].mean()]

        state_ind = np.argmin(abs(state_coords['longitude'].values-avg_gps[0])+abs(state_coords['latitude'].values-avg_gps[1]))
        state = state_coords.index[state_ind]
        
    except:
        state = 'ZZ'
        
    return state