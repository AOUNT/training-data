#Sentiment analysis script

import logging
import argparse
import json

import os
from googleapiclient.discovery import build

from pyspark import SparkContext
sc = SparkContext("local", "Simple App")

'''
You must set these values for the job to run.
'''
APIKEY="YOUR API KEY"   # Put you own 
PROJECT_ID="YOUR PROJECT ID"  # Put you own 
BUCKET="YOUR BUCKET"   # Put you own 


def analyzeEntitySentiment(text):
    from googleapiclient.discovery import build
    lservice = build('language', 'v1', developerKey=APIKEY)

    response = lservice.documents().analyzeEntitySentiment(
        body={
            'document': {
                'type': 'PLAIN_TEXT',
                'content': text
            }
        }).execute()
    
    return response



comments = sc.wholeTextFiles("gs://{0}/reviews/reviews.txt".format(BUCKET))


rdd1 = comments.map(lambda x: analyzeEntitySentiment(x[1]))


rdd2 =  rdd1.flatMap(lambda x: x['entities'] )\
            .flatMap(lambda x: [(x['name'], x['type'], x['salience'], x['sentiment']['magnitude'], x['sentiment']['score'] )])

  
results = rdd2.take(30)


for item in results:
  print('name= ',item[0],' | type= ',item[1], ' | saliance= ',item[2], ' | magnitude= ',item[3], ' | score= ',item[4])
