import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore, db
import pandas as pd
import schedule
import time
from pytrends.request import TrendReq
import pytz
from datetime import datetime
import pandas as pd
from pyspark import SparkContext
from operator import add
from wordcloud import WordCloud
import requests
from firebase_admin import storage

# Login to Google. Only need to run this once, the rest of requests will use the same session.
pytrend = TrendReq()

cred = credentials.Certificate('./myjson/serviceAccountKey.json')

firebase_admin.initialize_app(cred, {'storageBucket': 'real-time-data-visualization.appspot.com'})

db = firestore.client()

collect_name = 'GoogleTrend'

sc = SparkContext("local", "Accumulator app") 

old_keylist = []

bucket = storage.bucket()

### sequential sending
def job():
    global collect_name
    global pytrend
    global sc
    global old_keylist
    #pytrend = TrendReq()
    trending_searches_df = pytrend.trending_searches(pn='taiwan')
    l = trending_searches_df.values.tolist()
    keylist = [item for sublist in l for item in sublist]
    if len(old_keylist) == 0:
        old_keylist = keylist
    else:
        single = {}
        new = [s for s in keylist if s not in old_keylist]
        single['keyword'] = new
        doc_ref = db.collection(collect_name).document('New')
        doc_ref.set(single)

    assert len(keylist) == 20
    i = 0
    result = ''
    while i < 2:
        # Create payload and capture API tokens. Only needed for interest_over_time(), interest_by_region() & related_queries()
        pytrend.build_payload(kw_list=keylist[5*i:5*(i+1)], cat=0, timeframe='now 1-H', geo='TW', gprop='')
        # Interest Over Time
        interest_over_time_df = pytrend.interest_over_time()
        index = interest_over_time_df.index
        tw = pytz.timezone('Asia/Taipei')
        try:
            interest_over_time_df.index = index.tz_localize(pytz.utc).tz_convert(tw).strftime('%Y-%m-%d %H:%M')
        except:
            i += 1
            continue

        for j in range(5):
            single = {}
            if i == 0:
                single['name'] = keylist[5*i+j]
                single['time'] = interest_over_time_df.index.values.tolist()
                single['click'] = interest_over_time_df[keylist[5*i+j]].values.tolist()
            
            rdd = sc.parallelize(interest_over_time_df[keylist[5*i+j]].values.tolist())
            sum_ = rdd.reduce(add)
            single['value'] = sum_
            result += (keylist[5*i+j]+' ')*sum_

            #single['value'] = sum(interest_over_time_df[keylist[5*i+j]])
            if i == 0:
                doc_ref = db.collection(collect_name).document('Top {:d}'.format(5*i+j+1))
                doc_ref.set(single)
        i += 1
    if result is not '':
        wordcloud = WordCloud(collocations=False, font_path='./simfang.ttf', width=800, height=800, margin=2).generate(result)
        wordcloud.to_file('wordcloud.png')
        image_data = open('./wordcloud.png','rb')
        blob = bucket.blob('wordcloud.png')
        #blob.upload_from_string(image_data,content_type='image/png')
        blob.upload_from_file(image_data)
        image_data.close()

    print('-----Success-----')

job()
schedule.every(2).minutes.do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
