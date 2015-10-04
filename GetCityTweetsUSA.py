# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 15:19:38 2015

@author: jesseclark
Script to collect twitter data using the tweepy api
Filtered by language and geo location
Using GetNTweets which is where the api lives


"""

import sys

import GetNTweets
import datetime
import time
import os

def create_name_from_date():
    # for storing in a text file
    # create a name based on date and time
    nlength = 2
    t0 = datetime.datetime.now()
    str1 =(str(t0.year) + '-' + number_to_string(t0.month,nlength) + '-' + number_to_string(t0.day,nlength) +
        '-'+ number_to_string(t0.hour,nlength)+'-'+number_to_string(t0.minute,nlength))    
    
    return str1
    
        
def number_to_string(number, nlength):
    # make sure there are enough leading 0's    
    # and return as a string
    strn = str(number)
    while len(strn) < nlength:
        strn = '0'+strn

    return strn        
    
    
if __name__ == '__main__':

    # this is to collect tweets from the us based on long lat and en
    # collects as json then save the file
    # control how many tweeets before a new file is created
    
    save_name = []
    # tweets before a new file is created
    tweets_total = 5000
    
    # a wait time after the tweets are collected (seconds)
    wait_time = 00 

    # how many files should be created
    ntimes = 5000 

    # save name
    save_name_base = 'Tweets'
    
    # save directory
    save_dir = './TimeDataUSA/'
    
    # check if exists
    if not os.path.isdir(save_dir):
        os.mkdir(save_dir)    
    
    #now loop and get the tweets
    for ii in range(0,ntimes):
        
        names = []
        # get a date name
        name0 = create_name_from_date()
        # append to list
        names.append(save_dir+'USA-'+name0+'.txt')   
        
        # here are the coords for USA - a little rough
        #1
        aa= [48.274636,  32.133527, 32.440768, 48.365264]
        ab=[ -126.725013 , -126.815871 , -85.020832 ,-84.793685]
        #2
        ba=[25.379009,  25.415995,  41.044785,40.642131]
        bb =[-79.687139 ,-99.667450 , -100.035939  , -71.457544]
        #3
        ca= [42.693160, 28.809587,  29.174332, 42.716732]
        cb= [ -90.550973, -90.807590 ,-68.449868,  -68.866870]
        
        # final coords, can use boxes to the twitter API
        cords = [min(ab),min(aa),max(ab),max(aa),min(bb),min(ba),max(bb),max(ba),min(cb),min(ca),max(cb),max(ca)]
        
        # use a try in case connection is borken, too many request etc
        try:
            print("USA \n")
            GetNTweets.collectTweetsLoc(tweets_total , names[0],cords )
        except:
            continue

