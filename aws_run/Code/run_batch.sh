#!/bin/bash

BATCH_ID=$1
BATCH_FILE="/home/ec2-user/batch.txt"

echo "Running batch: $BATCH_ID"

START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "$START_TIME" > /home/ec2-user/aws_run/results/start_time_${BATCH_ID}.txt

while IFS=',' read -r prov city; do
    echo "Processing: $prov, $city (batch $BATCH_ID)"
    python3 rent_curve.py --prov "$prov" --city "$city" --batch "$BATCH_ID"
done < "$BATCH_FILE"


END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "$END_TIME" > /home/ec2-user/aws_run/results/end_time_${BATCH_ID}.txt

echo "Batch $BATCH_ID completed."

aws s3 cp /home/ec2-user/aws_run/results/start_time_${BATCH_ID}.txt s3://final-project-bucket-e7037e/logs/
aws s3 cp /home/ec2-user/aws_run/results/end_time_${BATCH_ID}.txt   s3://final-project-bucket-e7037e/logs/

aws s3 cp /home/ec2-user/aws_run/results/welfare_result_batch_${BATCH_ID}.csv s3://final-project-bucket-e7037e/results/





