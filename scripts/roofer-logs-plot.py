import pandas as pd
from matplotlib import pyplot as plt

reconstruction_times = pd.read_csv("scripts/reconstruction-times.csv").drop_duplicates(subset=["building_id"])
reconstruction_times.set_index("building_id", inplace=True)

fig = plt.figure(figsize=(10,7))
fig.subplots_adjust(bottom=0.3)
p = reconstruction_times.median().plot(kind="bar")
plt.title("median")
plt.show()


fig = plt.figure(figsize=(10,7))
fig.subplots_adjust(bottom=0.3)
p = reconstruction_times.mean().plot(kind="bar")
plt.title("mean")
plt.show()

reconstruction_times["total"] = reconstruction_times.sum(axis=1)
above_10min = reconstruction_times[reconstruction_times["total"] > 600000]
above_10min.to_csv("scripts/reconstruction-times-above_10min.csv")