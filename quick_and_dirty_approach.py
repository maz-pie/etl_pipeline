#!/usr/bin/python3
from sys import prefix
from time import strftime
import pandas as pd
import boto3
from io import StringIO, BytesIO
from datetime import datetime, timedelta

#creating S3 instance to get data
s3 = boto3.resource('s3')
bucket = s3.Bucket('deutsche-boerse-xetra-pds')
#adding desired range of dates for which data will be loaded into the dataframe
date_range = ['2021-03-15','2021-03-16','2021-03-17']

'''
Another approach would be taking date as an arg instead of being fed in as an item of date_range table:
    date_argv = '2021-03-15'
since we can not get the data of present day, we will have to pull data from previous day by creating
a datetime object:
    date_argv_dt = datetime.strptime(date_argv, '%Y-%m-%d').date() - timedelta(days=1)

'''
#initializing dataframe to hold values gathered from S3 instance
df_all = pd.DataFrame()
#iterating over dates to collect all the data and append to the dataframe object
#for staging area
for date in date_range:
    bucket_obj = bucket.objects.filter(Prefix = date)
    #if using date_argv, we can use bucket.objects.all() method
    for i in bucket_obj:
        csv_obj = bucket.Object(key=i.key).get().get('Body').read().decode('utf-8')
        data = StringIO(csv_obj)
        df = pd.read_csv(data)
        df_all = pd.concat([df_all, df], ignore_index=True)
#print(df_all)
#cleaning dataframe
columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume']
df_all = df_all.loc[:,columns]
#print(df_all)
#drop empty row
df_all.dropna(inplace=True)

#Get opening price per ISIN and day
df_all['opening_price'] = df_all.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')
#check data
#print(df_all[df_all['ISIN']=='AT0000A0E9W5'])

#Get closing price per ISIN and day
df_all['closing_price'] = df_all.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('last')

#Aggregation
df_all = df_all.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'), minimum_price_eur=('MinPrice', 'min'), maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume', 'sum'))
#check data
#print(df_all)

#Percent Change Previous Closing
df_all['prev_closing_price'] = df_all.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
df_all['change_prev_closing_%'] = (df_all['closing_price_eur'] - df_all['prev_closing_price']) / df_all['prev_closing_price'] * 100
df_all.drop(columns=['prev_closing_price'], inplace=True)
df_all = df_all.round(decimals=2)
#check data
#print(df_all)

#Write and save report to S3 bucket
key = 'xetra_daily_report_' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet'
out_buffer = BytesIO()
df_all.to_parquet(out_buffer, index=False)
target_bucket = s3.Bucket('sample-etl-deutsche-boerse-xetra-pds-1111')
target_bucket.put_object(Body=out_buffer.getvalue(), Key=key)

'''
#read the uploaded file for verification
for obj in target_bucket.objects.all():
    print(obj.key)

prq_obj = target_bucket.Object(key='xetra_daily_report_20210510_101629.parquet').get().get('Body').read()
data = BytesIO(prq_obj)
df_report = pd.read_parquet(data)

'''