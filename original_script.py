from distutils.sysconfig import PREFIX
from http import client
from sys import prefix
import boto3, flatdict
import pandas as pd
import os
import csv, json
from datetime import datetime
from dotenv import load_dotenv

# Running function to parse .env file
load_dotenv()

# Function to retrieve current time and date and convert it to format for amazon bucket name
# def time_conversion():
#     todays_date = datetime.utcnow().strftime("%Y-%m-%d %H")
#     for r in (("-", "_"), (" ", "_")):
#         todays_date = todays_date.replace(*r)
#     return todays_date
    
# converted_date = time_conversion()

converted_date = '2022_08_17_02'



#Creating a low-level functional client
client = boto3.client(
    's3',
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_CODE'),
    region_name = os.getenv('AWS_REGION')
)
# Creating resource using boto3 since we want to check multiple buckets
# resource = boto3.resource(
#     's3',
#     aws_access_key_id = os.getenv('AWS_ACCESS_KEY'),
#     aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_CODE'),
#     region_name = os.getenv('AWS_REGION')
# )

# Getting list of all objects in nucket to assure same amount

# s3 = boto3.resource('s3')
# bucket = s3.Bucket(os.getenv('BUCKET_NAME'))
# all_objects = []
# for obj in bucket.objects.all():
#     all_objects.append(obj)

# print(len(all_objects))

# Fetch the list of existing buckets
clientResponse = client.list_buckets()

# Iterate through the list of json files + concatenate them to the rfid_df
total_dict = []

counter = 0
for resp in client.list_objects( Bucket = os.getenv('BUCKET_NAME'), Prefix = converted_date)['Contents']:
    dict_holder = {}
    # print(resp['Key'])
    file_name = client.get_object(
        Bucket = os.getenv('BUCKET_NAME'),
        Key = resp['Key']
    )
    test = file_name['Body'].read().decode('utf-8')
    my_dict = json.loads(test)[0]
    flat = flatdict.FlatDict(my_dict)

    for val in range(len(flat['payload:data'])):
        for k,v in flat.items():
            if k not in dict_holder:
                if k == 'payload:data':
                    for i,j in flat['payload:data'][val].items():
                        dict_holder[i] = j
                    continue
                dict_holder[k] = v
        total_dict.append(dict_holder)
        dict_holder = {}
    counter += 1

# print(counter)

csv_name = f'{converted_date}.csv'
with open(csv_name, 'w', encoding='utf-8', newline= '') as output_file:
    fc = csv.DictWriter(output_file, fieldnames=total_dict[0].keys())
    fc.writeheader()
    fc.writerows(total_dict)

# EXAMPLE PAYLOAD
#     [
#     {
#         "tenantName": "GREENVILLE",
#         "eventType": "EXIT",
#         "eventTimeEpoch": 1657151998,
#         "payload": {
#             "data": [
#                 {
#                     "antenna": "Antenna1",
#                     "datetime": "2022-07-06 18:53:19.0757",
#                     "readername": "Modjoul Test 2",
#                     "epc": "SCAN00000131",
#                     "deviceid": "C4:7D:CC:64:8E:9A",
#                     "user": "null",
#                     "tagevent": "Tag Visible",
#                     "peakrssi": -60
#                 },
#                 {
#                     "antenna": "Antenna1",
#                     "datetime": "2022-07-06 18:53:24.0198",
#                     "readername": "Modjoul Test 2",
#                     "epc": "SCAN00000131",
#                     "deviceid": "C4:7D:CC:64:8E:9A",
#                     "user": "null",
#                     "tagevent": "Tag Visible",
#                     "peakrssi": -64
#                 },
#                 {
#                     "antenna": "Antenna1",
#                     "datetime": "2022-07-06 18:53:16.0756",
#                     "readername": "Modjoul Test 2",
#                     "epc": "FSCN00000043",
#                     "deviceid": "C4:7D:CC:64:8E:9A",
#                     "user": "null",
#                     "tagevent": "Tag Visible",
#                     "peakrssi": -65
#                 },
#                 {
#                     "antenna": "Antenna1",
#                     "datetime": "2022-07-06 18:53:16.0907",
#                     "readername": "Modjoul Test 2",
#                     "epc": "FSCN00000042",
#                     "deviceid": "C4:7D:CC:64:8E:9A",
#                     "user": "null",
#                     "tagevent": "Tag Visible",
#                     "peakrssi": -60
#                 },
#                 {
#                     "antenna": "Antenna1",
#                     "datetime": "2022-07-06 18:53:16.0937",
#                     "readername": "Modjoul Test 2",
#                     "epc": "FSCN00000041",
#                     "deviceid": "C4:7D:CC:64:8E:9A",
#                     "user": "null",
#                     "tagevent": "Tag Visible",
#                     "peakrssi": -52
#                 },
#                 {
#                     "antenna": "Antenna1",
#                     "datetime": "2022-07-06 18:53:16.0606",
#                     "readername": "Modjoul Test 2",
#                     "epc": "FSCN00000040",
#                     "deviceid": "C4:7D:CC:64:8E:9A",
#                     "user": "null",
#                     "tagevent": "Tag Visible",
#                     "peakrssi": -61
#                 },
#                 {
#                     "antenna": "Antenna1",
#                     "datetime": "2022-07-06 18:53:24.0499",
#                     "readername": "Modjoul Test 2",
#                     "epc": "FSCN00000040",
#                     "deviceid": "C4:7D:CC:64:8E:9A",
#                     "user": "null",
#                     "tagevent": "Tag Visible",
#                     "peakrssi": -62
#                 },
#                 {
#                     "antenna": "Antenna1",
#                     "datetime": "2022-07-06 18:53:16.0577",
#                     "readername": "Modjoul Test 2",
#                     "epc": "SCAN00000135",
#                     "deviceid": "C4:7D:CC:64:8E:9A",
#                     "user": "null",
#                     "tagevent": "Tag Visible",
#                     "peakrssi": -67
#                 }
#             ],
#             "accountuuid": "14222956194918074368",
#             "version": "1.0",
#             "deviceid": "C4:7D:CC:64:8E:9A"
#         }
#     }
# ]

