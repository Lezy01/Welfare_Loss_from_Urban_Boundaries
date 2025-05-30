
import argparse
import boto3

def launch_ec2_instances(num_instances):

    ec2 = boto3.client('ec2', region_name='us-east-1')

    AMI_ID = 'ami-0c101f26f147fa7fd'  
    INSTANCE_TYPE = 't3.large'
    KEY_NAME = 'vockey'
    SECURITY_GROUP_IDS = ['sg-091cee62774be60fb']
    IAM_ROLE_NAME = 'LabInstanceProfile'  
    BUCKET_NAME = 'final-project-bucket-e7037e'


    for i in range(1, num_instances + 1):
        batch_filename = f"batch_{i}.txt"

        user_data_script = f"""#!/bin/bash
        cd /home/ec2-user

        sudo yum update -y
        sudo yum install -y python3 unzip git
        sudo yum install -y python3-pip
        pip3 install --user pandas geopandas numpy shapely matplotlib geopy statsmodels

        aws s3 cp s3://{BUCKET_NAME}/aws_run.zip aws_run.zip
        unzip aws_run.zip

        sudo chown -R ec2-user:ec2-user /home/ec2-user/aws_run
        mkdir -p /home/ec2-user/aws_run/results


        aws s3 cp s3://{BUCKET_NAME}/rent_curve.py rent_curve.py
        aws s3 cp s3://{BUCKET_NAME}/run_batch.sh run_batch.sh
        aws s3 cp s3://{BUCKET_NAME}/batches_aws/{batch_filename} batch.txt

        bash run_batch.sh {i} > run_batch_{i}.log 2>&1
        aws s3 cp run_batch_{i}.log s3://{BUCKET_NAME}/logs/


        sudo shutdown -h now
        """

        ec2.run_instances(
            ImageId=AMI_ID,
            InstanceType=INSTANCE_TYPE,
            KeyName=KEY_NAME,
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=SECURITY_GROUP_IDS,
            IamInstanceProfile={'Name': IAM_ROLE_NAME},
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {'Key': 'Purpose', 'Value': 'WelfareBatchProcessing'},
                        {'Key': 'BatchFile', 'Value': batch_filename}
                    ]
                }
            ],
            UserData=user_data_script,
            InstanceInitiatedShutdownBehavior='terminate'
        )
    print(f"Launched EC2 instance for each of the {num_instances} batches.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_ec2', type=int, default=8,
                        help='Number of EC2 instances to launch for processing the same number of input batch files (default: 8).')
    args = parser.parse_args()

    launch_ec2_instances(args.num_ec2)
