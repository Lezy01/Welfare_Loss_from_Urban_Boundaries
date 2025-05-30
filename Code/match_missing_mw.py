
tasks_path = "/home/xinyu01/Final_project/Code/tasks.txt"
tasks = pd.read_csv(tasks_path, header=None, names=["prov", "city", "val1", "val2"])

results_path = "/home/xinyu01/Final_project/Data/welfare_results_mw.csv"
results = pd.read_csv(results_path)


tasks["key"] = tasks["prov"].str.strip() + "," + tasks["city"].str.strip()
results["key"] = results["prov"].str.strip() + "," + results["city"].str.strip()

missing = tasks[~tasks["key"].isin(results["key"])][["prov", "city"]]

print("Missing tasks:")
for _, row in missing.iterrows():
    print(f"{row['prov']},{row['city']}")

missing.to_csv("missing_results.txt", index=False, header=False)
