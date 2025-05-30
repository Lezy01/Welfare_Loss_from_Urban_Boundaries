import pandas as pd
from datetime import datetime

df = pd.read_csv("times.txt", sep='|')
df = df.dropna(subset=["Start", "End"])
df["Start"] = pd.to_datetime(df["Start"])
df["End"] = pd.to_datetime(df["End"])

start = df["Start"].min()
end = df["End"].max()
elapsed = end - start

print(f"Start: {start}")
print(f"End:   {end}")
print(f"Total wall time: {elapsed}")
