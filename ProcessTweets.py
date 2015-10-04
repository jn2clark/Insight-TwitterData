# -*- coding: utf-8 -*-
"""
Created on Sat Sep 12 14:58:22 2015

Process the collected tweets and store into an SQL database
The number of days to procss can be set


@author: jesseclark
"""

from __future__ import print_function
from time import time
import sys
import LoadCleanTweets
import TrainClassifier
import glob
import os
import re
import datetime
import numpy as np
from sqlalchemy import create_engine
import sqlalchemy
import pymysql as mdb

def load_combine_txt(file_path = os.getcwd(), suffix = '*-p.txt', out_name = "combined.txt", read_files = []):
    # outname should be the full out path
    if len(read_files) == 0:
        print("\n Searching for names... \n")        
        read_files = glob.glob(file_path+'/'+suffix)
    else:
        print("\n Using supplied names... \n")        
        
    with open(out_name, "wb") as outfile:
        for f in read_files:
            with open(f, "rb") as infile:
                outfile.write(infile.read())
                

def get_date_from_name(fname, t_format = "%Y-%m-%d-%H-%M"):   
    # convert to datetime object so we can sort based on time
    try:
        tweet_time = datetime.datetime.strptime(fname[4:], t_format)
    except:
        tweet_time = ''
        
    return tweet_time

def get_thresh_date(N_days=5, t_format = "%Y-%m-%d-%H-%M"):
    # how many days before to keep        
    # get the current date need to make a datetime obj
    now = datetime.datetime.strptime(datetime.date.today().strftime(t_format), t_format) 
    # get threshold time
    now_thresh = now - datetime.timedelta(days=N_days)
    
    return now_thresh

def get_sql_datatypes():
    my_data_keys = ['id', 'text', 'created_at']#,'state','hashtag']
    # set the data types for the database
    # use these keys to make the frame to deposit
    my_data_types = {my_data_keys[0]: sqlalchemy.types.BIGINT,
                my_data_keys[1]: sqlalchemy.types.Text,
                my_data_keys[2]: sqlalchemy.types.Text,#
                }
                
    return my_data_types, my_data_keys


if __name__ == '__main__':

    # save to SQL database?
    to_sql = True
    dbname = 'Tweets'
    tablename = 'table2'
    
    # to use all set N_days -ve
    N_days = 1 
    # how much to sub-sample the data? (skip this many, 1 = all, 2=half et)
    N_skip = 15 
    # get the date to which to start processing files    
    thresh_date = get_thresh_date(N_days)
    
    # directories for data
    base_dir = '/'
    # dir for saving
    save_dir = '/'

    # classifier dir for sent
    cl_dir = 'clf.pkl'
    text_clf = TrainClassifier.load_classifier(cl_dir)    
    
    # now get the names of all files to analyze
    # we just want the filename 
    keys = glob.glob(base_dir+"USA-"+"*.txt")
    # remove it here
    keys = [os.path.basename(key).split(".txt")[0] for key in keys]
    
    #other key replacing the - for     
    skeys = [re.sub('-', '_', key) for key in keys]

    print(" ")    
    print(keys)    
    print(" ")    
    
    # store the file that were processed
    read_files_p = []    
    read_files_pP = []    
        
    if to_sql:
        print('\n Connecting to sql data base...')
        engine = create_engine('mysql+pymysql://root@localhost/'+dbname+'?unix_socket=/tmp/mysql.sock&charset=utf8')
        sql_datatypes,  dbkeys = get_sql_datatypes()        
        # shoule we replace existing table or append initially?
        sql_app_rep = 'replace'
    
    # load all the tweets into files
    for val, keytuple in enumerate(zip(keys, skeys)):
        # get the file names
        key, skey = keytuple

        # get date from file
        fdate = get_date_from_name(key)        
        
        # check if we should process the files
        if N_days < 0:
            process = True
        else:
            # is the file data > than the threshold
            if fdate > thresh_date:
                process = True
            else:
                process = False

        # check if we should not skip the file
        if np.mod(val, N_skip) == 0:
            no_skip = True
        else:
            no_skip = False
        
        if process and no_skip:        
            # usa a try as some files are empty or if overdo rate limit etc       
            try:
                print('\n'+str(val)+'\n')
                print(base_dir+key+'.txt')
                # make the class
                tweets = LoadCleanTweets.LoadCleanTweets()
                # load the file
                tweets.load_tweets(base_dir+key+'.txt')
                # take infor from json into pandas
                tweets.extract_to_data_frame()
                # make lower case
                tweets.make_lower_frame()
                # save the processed file
                tweets.save_tweets(base_dir+key+'.pPtxt')
                 
                # add the file to the list
                read_files_pP += [base_dir+key+'.pPtxt']
                
                # make lower and remove punct
                tweets.remove_punctuation_partial_frame()
    
                # make lower and remove punct
                tweets.add_space_partial_frame()
    
                # save to sql?
                if to_sql:
                    print('\n To sql...')
                    sql_frame = tweets.tweets_df[dbkeys]
                    sql_frame.to_sql(tablename, engine, if_exists=sql_app_rep, index=False, dtype=sql_datatypes) #append next time!
                    sql_app_rep = 'append'                
                    print('Done... \n')
                    del sql_frame
    
                tweets.utf_encode()
    
                # now save the processed tweets for processing
                #old one haad p at the end, change file prefix         
                tweets.save_tweets(base_dir+key+'.ptxt')          
                
                # add the file to the list
                read_files_p += [base_dir+key+'.ptxt']
                # delete the file to save memory (not a prob for small files)
                del tweets
            
            except:
                print("Error %s" % key)
                continue
        
        # if process else statemnt
        else:
            print("Skipping -[ %s ] - out of date range" % key)
            
            
    # now make the name based on if any temporal filtering was done
    if N_days < 0:
        fnames = [save_dir+"USAcombined.txt", save_dir+"USAcombinedOrig.txt"]
    else:
        fnames = [save_dir+"USAcombined-d"+str(N_days)+".txt", save_dir+"USAcombinedOrig-d"+str(N_days)+".txt"]
                
    print('Number of file % s', str(len(read_files_p)))                
                
    # now join them
    print(" ")
    print("Combining files...")
    load_combine_txt(file_path = base_dir, suffix = 'USA-*.ptxt', out_name = fnames[0], read_files =read_files_p)
    
    print(" ")
    print("Combining files...")
    load_combine_txt(file_path = base_dir, suffix = 'USA-*.pPtxt', out_name = fnames[1],  read_files =read_files_pP)
    
