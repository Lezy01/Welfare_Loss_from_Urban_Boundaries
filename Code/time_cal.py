import pandas as pd
from datetime import datetime

slurm_path = "/home/xinyu01/Final_project/Code/slurm_array_times.txt"
df = pd.read_csv(slurm_path, sep='|')
df = df.dropna(subset=["Start", "End"])
df["Start"] = pd.to_datetime(df["Start"])
df["End"] = pd.to_datetime(df["End"])

start = df["Start"].min()
end = df["End"].max()
elapsed = end - start

print(f"Start: {start}")
print(f"End:   {end}")
print(f"Slurm array running times: {elapsed}")
