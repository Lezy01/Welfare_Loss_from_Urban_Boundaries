import boto3
import os
import uuid
from botocore.exceptions import ClientError

base_bucket_name = "final-project-bucket"
unique_bucket_name = "final-project-bucket-e7037e"

region = "us-east-1"  

local_dir = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/aws_run"
s3_prefix = "aws_run"  


s3 = boto3.client("s3", region_name=region)
s3_resource = boto3.resource("s3")

try:
    if region == "us-east-1":
        s3.create_bucket(Bucket=unique_bucket_name)
    else:
        s3.create_bucket(
            Bucket=unique_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region}
        )
    print(f"Created bucket: {unique_bucket_name}")
except ClientError as e:
    print(f"Failed to create bucket: {e}")

"""for root, dirs, files in os.walk(local_dir):
    for file in files:
        local_path = os.path.join(root, file)
        relative_path = os.path.relpath(local_path, local_dir)
        s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")

        try:
            s3.upload_file(local_path, unique_bucket_name, s3_path)
            print(f"Uploaded: {relative_path} -> s3://{unique_bucket_name}/{s3_path}")
        except ClientError as e:
            print(f"Upload failed for {file}: {e}")
            
zip_path = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/aws_run.zip"
try:
    s3.upload_file(zip_path, unique_bucket_name, s3_path)
    print(f"Uploaded aws_run.zip to s3://{unique_bucket_name}/{s3_path}")
except ClientError as e:
    print(f" Upload failed: {e}")
"""

code_dir = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/aws_run/Code"

for script_file in ["rent_curve.py", "run_batch.sh"]:
    local_path = os.path.join(code_dir, script_file)
    s3_key = f"{script_file}"

    try:
        s3.upload_file(local_path, unique_bucket_name, s3_key)
        print(f"Uploaded {script_file} to s3://{unique_bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"Failed to upload {script_file}: {e}")

batches_dir = os.path.join(code_dir, "batches_aws")
for filename in os.listdir(batches_dir):
    if filename.endswith(".txt"):
        local_path = os.path.join(batches_dir, filename)
        s3_key = f"batches_aws/{filename}"

        try:
            s3.upload_file(local_path, unique_bucket_name, s3_key)
            print(f"Uploaded {filename} to s3://{unique_bucket_name}/{s3_key}")
        except ClientError as e:
            print(f"Failed to upload {filename}: {e}")


print("\nAll files uploaded.")
print(f"S3 Bucket: {unique_bucket_name}")
