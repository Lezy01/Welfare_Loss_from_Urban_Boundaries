import pandas as pd
import os

tasks_path = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Code/tasks.txt"
results_path = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Data/welfare_results_mw.csv"
log_dir = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Code/logs"

tasks = pd.read_csv(tasks_path, header=None, names=["prov", "city", "val1", "val2"])
results = pd.read_csv(results_path, header=None, names=["prov", "city", "val1", "val2"])

tasks["key"] = tasks["prov"].str.strip() + "," + tasks["city"].str.strip()
results["key"] = results["prov"].str.strip() + "," + results["city"].str.strip()


missing = tasks[~tasks["key"].isin(results["key"])][["prov", "city"]]


print("Missing tasks with log info:")
for _, row in missing.iterrows():
    prov, city = row["prov"], row["city"]
    key = f"{prov.strip()},{city.strip()}"

    arrayid = tasks[tasks["key"] == key].index[0] 
    target_suffix = f"_{arrayid}.out"

    matched_logs = [f for f in os.listdir(log_dir) if f.endswith(target_suffix)]

    if matched_logs:
        log_path = os.path.join(log_dir, matched_logs[0])
        with open(log_path, "r") as f:
            lines = f.readlines()
            last_line = lines[-1].strip() if lines else ""
            if last_line == f"No matching file found for {prov}-{city}.":
                print(f"{prov},{city}: No matching file found for {prov}-{city}.")
            else:
                print(f"{prov},{city}: Last log line = '{last_line}'")
    else:
        print(f"{prov},{city}: No log file found for arrayid {arrayid}")

missing.to_csv("missing_results_loc.txt", index=False, header=False)