import os
import csv
import pandas as pd
import numpy as np

# generate tasks for urban land rent curve fitting 
hp_dir = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Data/Cleaned/City_hp"
task_file_path = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Code/tasks.txt"
csvs = [f for f in os.listdir(hp_dir) if f.endswith("_hp.csv")]

with open(task_file_path, "w") as f:
    for file in csvs:
        name = file.replace("_hp.csv", "")
        prov, city = name.split("-", 1)
        f.write(f"{prov},{city}\n")

# create output directory for welfare results
output_file = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Data/welfare_results_mw.csv"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

with open(output_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["prov", "city", "loss", "loss_ratio"])

# split tasks into batches for parallel processing on EC2 instances
def split_tasks(task_file, num_batches):
    df = pd.read_csv(task_file, header=None, names=["prov", "city"])
    batches = np.array_split(df, num_batches)
    for i, batch in enumerate(batches, 1):
        batch.to_csv(f"/Users/yxy/UChi/Spring2025/MACS30123/Final_project/aws_run/Code/batches_aws/batch_{i}.txt", index=False, header=False)

split_tasks("/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Code/tasks.txt", num_batches=20)

