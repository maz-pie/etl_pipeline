from operator import index
import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timedelta

# defining functions:
#Adaptive layer
def read_csv_to_df(bucket, key, decoding = 'utf-8', sep = ','):
    csv_obj = bucket.Object(key=key).get().get('Body').read().decode(decoding)
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=sep)
    return df


def write_to_s3(bucket, df, key):
    out_file = BytesIO()
    df.to_parquet(out_file, index=False)
    bucket.put_object(Body=out_file.getvalue(), key=key)
    return True


def return_data_object(bucket, arg_date, src_format):
    min_date = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
    data_objects = [obj.key for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0], src_format).date() >= min_date]
    return data_objects


#Application layer
def extract(bucket, objects):
    df = pd.concat([read_csv_to_df(bucket, obj) for obj in objects], ignore_index=True)
    return df

def transform_report1(df, columns, arg_date):
    df = df.loc[:, columns]
    df.dropna(inplace=True)
    df['opening_price'] = df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')
    df['closing_price'] = df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('last')
    df = df.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'), minimum_price_eur=('MinPrice', 'min'), maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume', 'sum'))
    df['prev_closing_price'] = df.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
    df['change_prev_closing_%'] = (df['closing_price_eur'] - df['prev_closing_price']) / df['prev_closing_price'] * 100
    df.drop(columns=['prev_closing_price'], inplace=True)
    df = df.round(decimals=2)
    df = df[df.Date >= arg_date]
    return df

def load(bucket, df, trg_key, trg_format):
    key = trg_key + datetime.today().strftime("%Y%m%d_%H%M%S") + trg_format
    write_to_s3(bucket, df, key)
    return True

def etl_report1(src_bucket, trg_bucket, objects, columns, arg_date, trg_key, trg_format):
    df = extract(src_bucket, objects)
    df = transform_report1(df, columns, arg_date)
    load(trg_bucket, df, trg_key, trg_format)
    return True


# main function entrypoint

def main():
    # Parameters/Configurations
    # Later read config
    arg_date = '2021-05-09'
    src_format = '%Y-%m-%d'
    src_bucket = 'deutsche-boerse-xetra-pds'
    trg_bucket = 'sample-etl-deutsche-boerse-xetra-pds-1111'
    columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume']
    trg_key = 'xetra_daily_report_'
    trg_format = '.parquet'
    
    # Init
    s3 = boto3.resource('s3')
    bucket_src = s3.Bucket(src_bucket)
    bucket_trg = s3.Bucket(trg_bucket)
    
    # run application
    objects = return_data_object(bucket_src, arg_date, src_format)
    etl_report1(bucket_src, bucket_trg, objects, columns, arg_date, trg_key, trg_format)


# run
main()

'''
Reading the report:
trg_bucket = 'xetra-1234'
s3 = boto3.resource('s3')
bucket_trg = s3.Bucket(trg_bucket)
for obj in bucket_trg.objects.all():
    print(obj.key)

prq_obj = bucket_trg.Object(key='xetra_daily_report_20210511_125520.parquet').get().get('Body').read()
data = BytesIO(prq_obj)
df_report = pd.read_parquet(data)

print(df_report)
'''