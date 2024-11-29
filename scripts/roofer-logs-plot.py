import pandas as pd
from matplotlib import pyplot as plt

reconstruction_times = pd.read_csv("scripts/roofer-logs.csv", index_col="building_id")

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
