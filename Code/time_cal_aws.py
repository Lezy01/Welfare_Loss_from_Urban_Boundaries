import boto3
import datetime

BUCKET_NAME = 'final-project-bucket-e7037e'
PREFIX = 'logs/' 

s3 = boto3.client('s3')


response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
files = [obj['Key'] for obj in response.get('Contents', []) 
         if 'start_time_' in obj['Key'] or 'end_time_' in obj['Key']]

start_times = []
end_times = []

for file_key in files:
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    content = obj['Body'].read().decode('utf-8').strip()
    try:
        timestamp = datetime.datetime.strptime(content, "%Y-%m-%dT%H:%M:%SZ")
        if 'start_time_' in file_key:
            start_times.append(timestamp)
        elif 'end_time_' in file_key:
            end_times.append(timestamp)
    except ValueError:
        print(f"Failed to parse time in {file_key}: {content}")

if start_times and end_times:
    min_start = min(start_times)
    max_end = max(end_times)
    duration = max_end - min_start

    print(f"Total runtime:   {duration} ")
else:
    print(" No valid timestamps found.")
