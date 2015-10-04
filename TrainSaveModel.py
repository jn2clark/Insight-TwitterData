# -*- coding: utf-8 -*-
"""
Created on Sat Sep 12 22:03:50 2015

@author: jesseclark

Train and save the word2vec model
Takes in the processed tweets from ProcessTweets.py
Most of the params are adjustable
Will also make a hash table for quick lookup of 
tweets associated with #'s.
Removing stop-words can take quite a while (1 hr for 10 M tweets)
Training is about 10-20 mins per iteration


"""

import gensim
from gensim.summarization import summarize
from gensim.summarization import keywords
import sys
from stemming.porter2 import stem
from nltk.corpus import stopwords
import itertools
from collections import Counter
from random import shuffle
import os
import sys
import imp
import HollaFunctions
import pymysql as mdb
import LoadCleanTweets

if __name__ == '__main__':
    
    # train from the sql db
    use_sql=True
    # db name
    dbname='Tweets'
    # table name    
    table_name='table1'
    # which column    
    col_name='text'

    # max number of tweets to save in the hash table
    max_term=2
    # also do a randomized training version
    do_rand=True
    
    # do the filterd list (stop) removed?
    do_stop =False
    
    # training params
    nnet_size=200#500 #200, 500?
    min_count=15 #play with this 510 or 15?
    window=10  

    sample=1e-5
    iterations=10
    negative=0


    #where to save the model
    save_dir=''    
    # where the data is
    data_dir=''
    # model save name
    base_name="USmodel"

    other_str='-d26'
    # where the combined text data is
    fnames=['USAcombined'+other_str+'.txt']
    
    # save name
    save_model_keys=base_name+'-n'+str(nnet_size)+'-mc'+str(min_count)+ \
                    '-w'+str(window)+'-i'+str(iterations)+'-ng'+str(negative)+other_str
    
    if not use_sql:
        # train US basic
        print("\n Loading text documents...")
        document_usa=HollaFunctions.load_text(text_name=data_dir+fnames[0])
    else:
        print("\n Loading text from SQL...")
        document_usa=HollaFunctions.get_field_sql(dbname, table_name, col_name)
    print("\n Done...")
    
    # load the filtered list if it exists
    #try
    # train USA with stop word removal
    if do_stop:
        print("removing stop words...(might take a while)")
        filtered_usa=[[word for word in sentance if word not in stopwords.words("english")] for sentance in document_usa]
        print('Done... \n')
    
    # do the hash tables now
    print('\n Preparing Hash table... ')
    htable=HollaFunctions.hash_tweets(document_usa, mterm='#')
    # save the table with the original text
    HollaFunctions.save_object(htable, save_dir+'USAhtable'+other_str+'.pkl')
    print('\n Preparing Shortened Hash tables... ')
    htable=HollaFunctions.shorten_hash_tweets(htable, max_term=max_term, shuffle_tweets=True)
    HollaFunctions.save_object(htable, save_dir+'USAhtable-short'+other_str+'.pkl')
    del htable 


    if not do_rand:
        # now do training
        print("Training USA model... ")
        print(data_dir+fnames[0])
        modelUSA=HollaFunctions.train_and_save_model(document_usa, nnet_size=nnet_size,  
                                           min_count=min_count, window=window, sample=sample, iterations=iterations, negative=negative, 
                                           save_name=save_dir+save_model_keys)
        print(save_dir+save_model_keys)
        print(" ")
        
        if do_stop:
            print("Training filtered USA model... \n") 
            modelUSA=HollaFunctions.train_and_save_model(filtered_usa, nnet_size=nnet_size,  
                                               min_count=min_count, window=window, sample=sample, iterations=iterations, negative=negative, 
                                               save_name=save_dir+save_model_keys+'Stop')
            print(save_dir+save_model_keys+'Stop')
        
    if do_rand:
        print("Training randomized USA model... \n") 
        print(data_dir+fnames[0])

        # randomize tweet order so they are not in chrono order
        # shuffle the docs
        shuffle(document_usa)

        modelUSA=HollaFunctions.train_and_save_model(document_usa, nnet_size=nnet_size,  
                                               min_count=min_count, window=window, sample=sample, iterations=iterations, negative=negative, 
                                               save_name=save_dir+save_model_keys+'Rand')
        print(save_dir+save_model_keys)
        print(" ")


        if do_stop:
            # train USA with stop word removal
            print("Training randomized filtered USA model... \n") 
            shuffle(filtered_usa)
            modelUSA=HollaFunctions.train_and_save_model(filtered_usa, nnet_size=nnet_size,  
                                                   min_count=min_count, window=window, sample=sample, iterations=iterations, negative=negative, 
                                                   save_name=save_dir+save_model_keys+'Stop'+'Rand')
            print(save_dir+save_model_keys+'Stop'+'Rand')
        
        print('\n Finished randomized training...')
        
    print('\n Finished everything!')
        
        
