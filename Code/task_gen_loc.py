import os
import csv

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
