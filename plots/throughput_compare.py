import argparse
import numpy as np
import pandas as pd
import time
import random
import matplotlib as mpl
import matplotlib.pyplot as plt
import glob
import os

from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Plot and compare throughput between HopsFS and \lambdaMDS.  #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

plt.style.use('ggplot')
mpl.rcParams['text.color'] = 'black'
mpl.rcParams['xtick.color'] = 'black'
mpl.rcParams['ytick.color'] = 'black'
mpl.rcParams["figure.figsize"] = (8,6)

font = {'weight' : 'bold',
        'size'   : 16}
mpl.rc('font', **font)

parser = argparse.ArgumentParser()

parser.add_argument("-ih", "--input-hopsfs", dest="input_hopsfs", default = "./ALL_DATA.txt", help = "Path to file containing ALL data.")
parser.add_argument("-il", "--input-lambdamds", dest="input_lambdamds", default = "./ALL_DATA.txt", help = "Path to file containing ALL data.")
parser.add_argument("-d", "--duration", default = 60, type = int, help = "Duration of the experiment in seconds.")
parser.add_argument("-c", "--columns", default = ["timestamp", "latency", "worker_id", "path"], nargs='+')

args = parser.parse_args()

input_hopsfs = args.input_hopsfs
input_lambdamds = args.input_lambdamds
duration = args.duration
COLUMNS = args.columns

print(COLUMNS)

def get_data(input_path, label = None):
    # If we pass a single .txt file, then just create DataFrame from the .txt file.
    # Otherwise, merge all .txt files in the specified directory.
    if input_path.endswith(".txt"):
        df = pd.read_csv(input_path)
    else:
        print("input_path: " + input_path)
        print("joined: " + str(os.path.join(input_path, "*.txt")))
        all_files = glob.glob(os.path.join(input_path, "*.txt"))
        li = []

        # Merge the .txt files into a single DataFrame.
        for filename in all_files:
            print("Reading file: " + filename)
            tmp_df = pd.read_csv(filename, index_col=None, header=0)
            tmp_df.columns = COLUMNS
            li.append(tmp_df)
        df = pd.concat(li, axis=0, ignore_index=True)
        df.columns = COLUMNS

    # Sort the DataFrame by timestamp.
    print("Sorting now...")
    start_sort = time.time()
    df = df.sort_values('timestamp')
    print("Sorted dataframe in %f seconds." % (time.time() - start_sort))

    np.savetxt('./' + label + "_latency.txt", df['latency'].values, fmt='%d')

    if 1 > 0:
        return []

    min_val = min(df['timestamp'])
    max_val = max(df['timestamp'])
    print("max_val - min_val =", max_val - min_val)
    print("max_val - min_val =", (max_val - min_val) / 1e9)
    def adjust(x):
        return (x - min_val) / 1e9

    # Sometimes, there's a bunch of data with WAY different timestamps -- like, several THOUSAND
    # seconds different. So, I basically adjust all of that data so it fits within the interval
    # of the rest of the data.
    df['ts'] = df['timestamp'].map(adjust)
    df2 = df[((df['ts'] >= duration+5))]
    min_val2 = min(df2['ts'])

    def adjust2(x):
        if x >= min_val2:
            return x - min_val2
        return x

    df['ts'] = df['ts'].map(adjust2)

    print("Total number of points: %d" % len(df))

    # For each second of the workload, count all the data points that occur during that second.
    # These are the points that we'll plot.
    buckets = [0 for _ in range(0, duration + 1)]
    total = 0
    for i in range(1, duration + 1):
        start = i-1
        end = i
        res = df[((df['ts'] >= start) & (df['ts'] <= end))]
        #print("%d points between %d and %d" % (len(res), start, end))
        buckets[i] = len(res)
        total += len(res)

    np.savetxt('./' + label + "_latency.txt", df['latency'].values, fmt='%d')
    return buckets

hopsfs_buckets = get_data(input_hopsfs, label = "HopsFS")
lambdamds_buckets = get_data(input_lambdamds, label = "LambdaMDS")

fig, axs = plt.subplots(nrows = 1, ncols = 1, figsize=(12,8))

axs.plot(list(range(len(hopsfs_buckets))), hopsfs_buckets, label = "HopsFS")
axs.plot(list(range(len(lambdamds_buckets))), lambdamds_buckets, label = r'$\lambda$' + "MDS")
axs.legend()
axs.set_xlabel("Time (seconds)")
axs.set_ylabel("Throughput (ops/sec)")

plt.show()