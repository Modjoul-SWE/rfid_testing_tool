from flask import Flask, render_template, redirect, request, url_for, jsonify
from distutils.sysconfig import PREFIX
from http import client
from sys import prefix
import boto3, flatdict
import pandas as pd
import os
import csv, json
from datetime import datetime
from dotenv import load_dotenv

from datetime import datetime
app = Flask(__name__)

total_dict = []

@app.route('/')
def home():
    return render_template("home.html")

@app.route('/time', methods=['POST'])
def search_time():

    # Establishing connection with S3
    account = request.form['aws_account']
    client = boto3_info(account=account)
    
    # Check to confirm connection with S3
    if client == None:
        print("server.py: line 27: S3 connection unsuccessful")
        return redirect('/')

    # formatting the date and time from form
    date = request.form['date'].replace("-", "_")

    # Date Starting
    start_time = request.form['start_time'].split(":")
    start_hour = start_time[0]
    date_start = f'{date}_{start_hour}'

    # End Time
    end_time = request.form['end_time'].split(":")
    end_minute = end_time[1]

    # Initializing bucket variable
    bucket = None

    # Setting production bucket name
    if request.form['bucket'] == "production" and account == "production":
        bucket = os.getenv("PROD_BUCKET_NAME")
    elif request.form['bucket'] == "non-whitelisted" and account == "production":
        bucket = os.getenv("PROD_NON_WHITELISTED_BUCKET_NAME")
    elif account == "production":
        bucket = os.getenv("PROD_GARBAGE_BUCKET_NAME")

    # Setting development bucket name
    if request.form['bucket'] == "production" and account == "dev":
        bucket = os.getenv("DEV_BUCKET_NAME")
    elif request.form['bucket'] == "non-whitelisted" and account == "dev":
        bucket = os.getenv("DEV_NON_WHITELISTED_BUCKET_NAME")
    elif account == "dev":
        bucket = os.getenv("DEV_GARBAGE_BUCKET_NAME")

    
    # Creating proper pre-fix for the s3 iteration
    if request.form['end_time'] < request.form['start_time']:
        return redirect("/")
    else:
        total_dict = []
        for time in range(int(start_time[1]), int(end_minute)+1):
            time = str(time).zfill(2)
            date_time = date_start + '_' + time
            try:
                bucket_data = s3_bucket_call(client=client, bucket=bucket, date_time=date_time)
                if bucket_data != None and bucket == os.getenv("PROD_BUCKET_NAME"):
                    total_dict.extend(bucket_data)
                elif bucket_data != None and bucket == os.getenv("PROD_NON_WHITELISTED_BUCKET_NAME"):
                    total_dict.append(bucket_data)
                elif bucket_data != None and bucket == os.getenv("DEV_BUCKET_NAME"):
                    total_dict.extend(bucket_data)
                elif bucket_data != None and bucket == os.getenv("DEV_NON_WHITELISTED_BUCKET_NAME"):
                    total_dict.append(bucket_data)
            except:
                continue
    if len(total_dict) != 0:
        return render_template('/table.html', total_dict=total_dict)
    else:
        return redirect("/")


# Establish connection to boto3 client
# Returns connection to either dev account or production account
def boto3_info(account):
    #Creating a low-level functional client
    if account == 'dev':
        client = boto3.client(
        's3',
        aws_access_key_id = os.getenv('AWS_DEV_ACCESS_KEY'),
        aws_secret_access_key = os.getenv('AWS_DEV_SECRET_ACCESS_CODE'),
        region_name = os.getenv('AWS_REGION')
        )
        return client
    elif account == 'production':
        client = boto3.client(
        's3',
        aws_access_key_id = os.getenv('AWS_PROD_ACCESS_KEY'),
        aws_secret_access_key = os.getenv('AWS_PROD_SECRET_ACCESS_CODE'),
        region_name = os.getenv('AWS_REGION')
        )
        return client
    

def s3_bucket_call(client, bucket, date_time):
    try:
        # print(client)
        # print(bucket) 
        # print(date_time)
        # print(client.list_objects(Bucket=bucket))
        for resp in client.list_objects(Bucket=bucket, Prefix=date_time)['Contents']:
            dict_holder = {}
            print("Initial success")
            file_name = client.get_object(
                Bucket=bucket,
                Key=resp['Key']
                )
            file_body = file_name['Body'].read().decode('utf-8')
            my_dict = json.loads(file_body)[0]
            flat = flatdict.FlatDict(my_dict)
            

            # for val in range(len(flat[payload])):
            if bucket == os.getenv("PROD_NON_WHITELISTED_BUCKET_NAME"):
                for key, val in flat.items():
                    if key not in dict_holder:
                        dict_holder[key] = val
                return dict_holder
            elif bucket == os.getenv("DEV_NON_WHITELISTED_BUCKET_NAME"):
                for key, val in flat.items():
                    if key not in dict_holder:
                        dict_holder[key] = val
                return dict_holder
            else:
                temp_dict = []
                payload = "payload:data"
                for val in range(len(flat[payload])):
                    for k,v in flat.items():
                        if k not in dict_holder:
                            if k == payload:
                                for i,j in flat[payload][val].items():
                                    dict_holder[i] = j
                                continue
                            dict_holder[k] = v   
                    temp_dict.append(dict_holder)
                return temp_dict
    except Exception as e:
        print("server.py: line 157:", e)
        return


if __name__ == "__main__":
    app.run(debug=True, port = 5001)
