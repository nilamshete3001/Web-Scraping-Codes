# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 11:25:32 2016
@author: Nilam
"""

import psycopg2
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
import itertools
import nltk
from nltk import FreqDist
import re
import csv
from nltk.stem import PorterStemmer

tokenizer = RegexpTokenizer(r'\S+')        
stop_words = set(stopwords.words('english'))
port = PorterStemmer()
        
conn = psycopg2.connect(database="pmtime", user="postgres", password="postgres", host="127.0.0.1", port="5432")
cur = conn.cursor()
cur.execute("select distinct best_year from monoclonal_proc")
year = cur.fetchall()
years = list(itertools.chain.from_iterable(year))

for y in years:
    yr = str(y)
    print("year", yr)
    cur.execute("""select abstract from monoclonal_proc where best_year=%s""", (y,))
    data = cur.fetchall()
    tokens = []
    
    tags=[]
    goID = ()
    
    
    for doc in data:
        doc1 = str(doc).lower()        
        doc1 = re.sub('[,;$.\\$#@!&^()*\'-/]', '', doc1)
        doc1 = re.sub('[\s+]', ' ', doc1)        
        list_of_words = [i for i in tokenizer.tokenize(doc1) if i not in stop_words]
        #print(list_of_words)
        doc2 = ([port.stem(i) for i in list_of_words])
        list_of_words2 = [i for i in (doc2) if i not in stop_words]
        bigramssss = nltk.bigrams(doc2)
        tokens.append(list(bigramssss))        
    
    #print("tokens", tokens)
    for item in list(itertools.chain.from_iterable(tokens)):
        if re.findall(r'<\w+:\d+>', item[0]):
            tags.append(item[0])
        elif re.findall(r'<\w+:\d+>', item[1] ):
            tags.append(item[1])
    
    goTag = tuple(set(tags))
    #print((goTag,))
        
    cur.execute("""select tag, prefterm from allsynsgo where tag in %s""",(goTag,))
    goData = cur.fetchall()
        
    synom = list(set(goData))
    #print(synom)
    
    firstTokens = []
    finalTokens = []

    for tag in synom:
        for item in list(itertools.chain.from_iterable(tokens)):
            if tag[0] in item:
                if item[0] == tag[0]:
                    firstTokens.append([(tag[1],item[1])])
                elif item[1] == tag[0]:
                    firstTokens.append([(item[0], tag[1])])
            else:
                firstTokens.append([item])
    #print((firstTokens)[1:30])            
    for tag in synom:
        for item in list(itertools.chain.from_iterable(firstTokens)):
            if tag[0] in item:
                if item[0] == tag[0]:
                    print(item[0],tag[0])
                    finalTokens.append([(tag[1],item[1])])
                elif item[1] == tag[0]:
                    print(item[1],tag[0])
                    finalTokens.append([(item[0], tag[1])])
            else:
                finalTokens.append([item])
                
    #print(list(itertools.chain.from_iterable(finalTokens)))
    fd = nltk.FreqDist(list(itertools.chain.from_iterable(firstTokens))) 
    file1 = csv.writer(open("F:\\Nilam\\MSBA_Degree\\CISI\\Pubmed_Postgres\\Output\\"+yr+"_ngram.csv", 'w', newline =''))
    for key, count in fd.most_common(100):
        file1.writerow([key, count])
    
    #print(list(itertools.chain.from_iterable(tokens)))"""
conn.commit();