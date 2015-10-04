# -*- coding: utf-8 -*-
"""
Created on Thu Sep 17 15:03:26 2015

@author: jesseclark

These are helper functions for training the model
Querying it and a few other things

"""


import gensim
from gensim.summarization import summarize
from gensim.summarization import keywords
import sys
from stemming.porter2 import stem
from nltk.corpus import stopwords
import os
import itertools
from collections import Counter
import cPickle 
from random import shuffle
import pymysql as mdb
import LoadCleanTweets


def save_object(clf,fname):
    # save the classifier
    with open(fname, 'wb') as fid:
        cPickle.dump(clf, fid)    
    

def load_object(fname):
    # load it again
    with open(fname, 'rb') as fid:
        clf = cPickle.load(fid)    
    return clf

def hash_tweets(document,mterm = '#'):
    # create a hashtable for the tweets

    # create hash table 
    htable = {}
    mterm = '#'

    for tweet in document:
        # check for the presence of a hashtag
        if mterm in " ".join(tweet):
            # now extract the htags
            htags =[tag for tag in tweet if mterm in tag]
            # now cycle through the htags and store the tweet
            for tag in htags:
                # check if the key is already there
                # if it is, append tweet to list
                if tag in htable:
                    htable[tag].append([" ".join(tweet)])
                # else create new key and tweet list
                else:
                    htable[tag] = [" ".join(tweet)]
    
    return htable

def shorten_hash_tweets(htable, max_term = -1,shuffle_tweets = True):
    # optionally set a max number to store to 
    # reduce mem (max_term < 0 will keep them all, default)
    
    # have the option to redue number of tweets for each entry, saves mem
    # only shorten if the number is +ve, allows us to easily avoid shortening
    if max_term > 0:
        for key in htable:
            if len(htable[key]) > max_term:
                if shuffle_tweets:
                    shuffle(htable[key])
                    
                htable[key] = htable[key][:max_term]
                
    return htable
    
def modelCount(document,term = '#',nret=10):
    # a simple counting algorithm for finding most popular term in tweets
    matched_tweets = get_tweets_match_document(document,term)

    hashtags = get_words_match_tweet(document,term = '#')

    # sort based on alphabet
    n_dict = sorted(Counter(matched_tweets).items(),key = MyFn1,reverse=True)

    # remove stop words
    n_dict = [ xx for xx in n_dict if xx[0] not in stopwords.words('english')]    
    # return hashtags
    hashtags = [xx for xx in n_dict if xx[0].startswith("#")]
    words = [xx for xx in n_dict if '#' not in xx[0]]

    return words[:nret],hashtags[:nret]

def MyFn1(s):
    # use this for custom sorting of dicts with sorted command    
    return s[1]

def get_tweets_match_document(document,term = '#'):
    # return tweets that match a keyword
    matches = [[word for word in sentance] for sentance in document if term in sentance]
    # need to flatten the list
    chain = itertools.chain(*matches)    
    
    return list(chain)

def get_words_match_tweet(document,term = '#'):
    # return words that match string
    matches = [word for word in document if term in word]
    
    return matches


def get_words_hashtags(modelout,nreturn=100):
    # get the words and hastags from the word2vec output
    words = [word[0] for word in modelout if '#' not in word[0]]
    hashtags = [word[0] for word in modelout if '#' in word[0]]
    numbers = [word[1] for word in modelout]
    return words[:nreturn],hashtags[:nreturn],numbers[:nreturn]

def query_model(model,query,nreturn = 10,nget=100):
    # qury the word2vec model for the most similar terms
    # nget is how many from the model and nreturn is how many
    # this function returns
    results = model.most_similar(positive=query,topn=nget)
    ww,hh,nn = get_words_hashtags(results)
    
    return ww[:nreturn],hh[:nreturn],nn[:nreturn]

def search_tweets(terms,document):
    #filter and return a string of all tweets
    # filter based on term
    w_term = [doc for doc in document if terms in doc]
    n_tweets = len(w_term)
    w_term =  [' '.join(word)+' . ' for word in w_term]
    w_term = ''.join(w_term)

    return w_term,float(n_tweets)


def summarize_tweets(text,ntweets=30,word_count =100):
    # this was to summarize the tweets for a particular topic or #
    text_sum = summarize(". ".join(text.split(".")[:ntweets]),word_count=word_count,ratio=0.5)

    return text_sum
    
def load_model_and_text(model_name ='',text_name=''):
    # load the word2vec model and text file
    # leave name blank to not load
    model = []
    text = []
    if len(model_name) > 1:    
        print("Loading model -[ %s ]" % model_name)    
        model = gensim.models.Word2Vec.load(model_name)
        print("Done")
        print(' ')
        print("Loading text -[ %s ]" % text_name)
       
    if len(text_name) > 1:
        with open(text_name, "r") as myfile:
            text = [line.rstrip().split() for line in myfile]
        print("Done")
        print(' ')

    return model,text

def load_model(model_name =''):
    # load the word2vec model and text file
    # leave name blank to not load
    print("Loading model -[ %s ]" % model_name)    
    model = gensim.models.Word2Vec.load(model_name)
    print("Done")
    print(' ')
    
    return model
    
def load_text(text_name=''):
    # load the word2vec model and text file
    # leave name blank to not load
    model = []
    text = []
    
    # read in the processed tweets
    if len(text_name) > 1:
        with open(text_name, "r") as myfile:
            text = [line.rstrip().split() for line in myfile]
        print("Done")
        print(' ')

    return text

def train_and_save_model(text,nnet_size = 200, min_count = 15, 
                         window = 10,save_name = '',sample = 0,iterations = 1,
                         negative = 0,replace=True):
    # leave save_name a empty to not save
    # make a model
    print("Training model...")
    modeln = gensim.models.Word2Vec(text, size=nnet_size, window=window, 
                                       min_count=min_count, workers=4,sample=sample,
                                       iter=iterations,negative=negative)
    # any more training? saves mem if not (defualt)
    modeln.init_sims(replace=replace)
    print("Done...")
    print(" ")
    
    if len(save_name) > 1:
        print("Saving model [%s]" % save_name)
        modeln.save(save_name)

    return modeln

def get_missing_word(model,a,b,x):
    # use this to text the addition and subtraction of the model    
    predicted = model.most_similar([x, b], [a])[0][0]
    print("'%s' is to '%s' as '%s' is to '%s'" % (a, b, x, predicted))

    return predicted


class multiModels:
    #class for loading and quering multiple models at once
    # was going to use this for different cities etc
    def __init__(self,model_keys,model_dir):
        self.model_keys = model_keys    
        self.model_dir = model_dir
        
    def load_mult_models(self):
        # here we do the loading and call assign a param in the struct        
        for key in model_keys:
            estr = "self."+key+ ", a = load_model_and_text('"+model_dir+key+"')"
            print(estr)
            exec estr
            
    def get_words_hashtags_model(self,words,nret = 10):
        # lets get the results from the words
        if type(words) == str:
            words = list(words)
        for key in self.model_keys:
            #estr = "self."+key+ ".words,self."+key+ ".hashtags  =  \
            #        query_model(self."+key+",'"+words+"',100)"
            estr = "self."+key+ ".words,self."+key+ ".hashtags,self."+key+".sim_nums  =  \
                    query_model(self."+key+","+'["'+'","'.join(words)+'"]'+","+str(nret)+")"
            print(estr)
            exec estr

    
def stem_documents(document):
    # stem a document (list of lists)
    stemmed = [[stem(word) for word in sentence if "#" not in word] for sentence in document]    
    return stemmed    
 
def remove_stop_words_document(document):   
    # remove stop words from a doc (list of list)
    filtered = [word for word in document if word not in stopwords.words('english')]
    return filtered
    

def hashtag_url_gen(hashtags):
    # use this to generate a twitter url
    base_url = 'https://twitter.com/hashtag/'
    urls = [base_url+hashtag[1:] for hashtag in hashtags]
    
    return urls
    
def load_combine_txt(file_path = os.getcwd(),suffix = '*-p.txt',out_name = "combined.txt",read_files = []):
    # outname should be the full out path
    if len(read_files) == 0:
        print("\n Searching for names... \n")        
        read_files = glob.glob(file_path+'/'+suffix)
    else:
        print("\n Using supplied names... \n")        
        
    print(read_files)
    with open(out_name, "wb") as outfile:
        for f in read_files:
            with open(f, "rb") as infile:
                outfile.write(infile.read())
                
def save_list_to_text(save_list, file_path = "./Classify/test.txt"):
    # save as a simple text file with new line for reading into R
    counter = 0
        
    with open(file_path, 'w') as thefile:        
        for item in save_list:
            thefile.write(item+' \n')
    
    with open(file_path, "a") as myfile:
        myfile.write("\n")
        
def get_field_sql(dbname,table_name,col_name):
    # get a field from the sql database    
    con = mdb.connect(db=dbname, user='root', passwd='', unix_socket="/tmp/mysql.sock")
    with con:
    
       cur = con.cursor()
       cur.execute("SELECT "+col_name+" FROM "+table_name)
       rows = cur.fetchall()
    
       document = []
    
       for row in rows:
           #print row
           document.append(LoadCleanTweets.remove_punctuation_partial(str(row)).split(" "))
           
    return document