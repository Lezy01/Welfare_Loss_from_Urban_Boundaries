import boto3

ec2 = boto3.resource('ec2')

user_data_script = '''#!/bin/bash
sudo apt update
sudo apt install -y python3-pip git parallel
pip3 install pandas geopandas shapely matplotlib geopy statsmodels
'''

instances = ec2.create_instances(
    ImageId='ami-0abcdef1234567890',  # 用你选的 AMI ID
    MinCount=1,
    MaxCount=1,
    InstanceType='t3.medium',
    KeyName='your-key-name',
    SecurityGroupIds=['your-sg-id'],
    UserData=user_data_script,
)

print("Instance created:", instances[0].id)
