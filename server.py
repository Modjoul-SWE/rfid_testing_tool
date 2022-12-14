from flask import Flask, render_template, redirect, request
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

@app.route('/')
def home():
    return render_template("home.html")

@app.route('/time', methods=['POST'])
def search_time():
    # Establishing connection with S3
    client = boto3_info()
    # formatting the date and time from form
    date = request.form['date'].replace("-", "_")
    # Date Starting
    start_time = request.form['start_time'].split(":")
    start_hour = start_time[0]
    date_start = f'{date}_{start_hour}'

    # End Time
    end_time = request.form['end_time'].split(":")
    end_minute = end_time[1]

    # Setting bucket name
    if request.form['bucket'] == "production":
        bucket = os.getenv("PROD_BUCKET_NAME")
    elif request.form['bucket'] == "non-whitelisted":
        bucket = os.getenv("NON_WHITELISTED_BUCKET_NAME")
    else:
        bucket = os.getenv("GARBAGE_BUCKET_NAME")
    
    # Creating proper pre-fix for the s3 iteration
    if request.form['end_time'] < request.form['start_time']:
        print("redirected because the dates were incorrect")
        return redirect("/")
    else:
        total_dict = []
        for time in range(int(start_time[1]), int(end_minute)+1):
            time = str(time).zfill(2)
            date_time = date_start + '_'+ time
            try:
                bucket_data = s3_bucket_call(client=client, bucket=bucket, date_time=date_time)
                if bucket_data != None and bucket == os.getenv("PROD_BUCKET_NAME"):
                    total_dict.extend(bucket_data)
                elif bucket_data != None and bucket == os.getenv("NON_WHITELISTED_BUCKET_NAME"):
                    total_dict.append(bucket_data)
            except:
                print("In first exception")
                continue
    if len(total_dict) != 0:
        return render_template('/table.html', total_dict=total_dict)
    else:
        return redirect("/")


def boto3_info():
    #Creating a low-level functional client
    client = boto3.client(
    's3',
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_CODE'),
    region_name = os.getenv('AWS_REGION')
    )
    
    return client

def s3_bucket_call(client, bucket, date_time):
    try: 
        for resp in client.list_objects(Bucket=bucket, Prefix=date_time)['Contents']:
            dict_holder = {}
            file_name = client.get_object(
                Bucket=bucket,
                Key=resp['Key']
                )
            file_body = file_name['Body'].read().decode('utf-8')
            my_dict = json.loads(file_body)[0]
            flat = flatdict.FlatDict(my_dict)
            

            # for val in range(len(flat[payload])):
            if bucket == os.getenv("NON_WHITELISTED_BUCKET_NAME"):
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
                    # print(f"\n{dict_holder}")     
                    temp_dict.append(dict_holder)
                return temp_dict
    except:
        print("In second exception")
        return
        
def convert_csv(total_dict):
    csv_name = 'results.csv'
    with open(csv_name, 'w', encoding='utf-8', newline= '') as output_file:
        fc = csv.DictWriter(output_file, fieldnames=total_dict[0].keys())
        fc.writeheader()
        fc.writerows(total_dict)


if __name__ == "__main__":
    app.run(debug=True, port = 5001)
