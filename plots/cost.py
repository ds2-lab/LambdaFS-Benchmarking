import argparse
import numpy as np
import pandas as pd
import time
import random
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import glob
import os
from tqdm import tqdm

from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset

# # # # # # # # # # # # # # # # # #
# Plot cost for \lambdaMDS. #
# # # # # # # # # # # # # # # # # #

plt.style.use('ggplot')
mpl.rcParams['text.color'] = 'black'
mpl.rcParams['xtick.color'] = 'black'
mpl.rcParams['ytick.color'] = 'black'
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
font = {'weight' : 'bold',
        'size'   : 26}
mpl.rc('font', **font)

parser = argparse.ArgumentParser()

parser.add_argument("-i", "--input", default = "./ALL_DATA.txt", help = "Path to folder containing the data.")
parser.add_argument("-l", "--label", default = r'$\lambda$' + "MDS", help = "Label for first set of data.")
parser.add_argument("-d", "--duration", default = 300, type = int, help = "Duration of the experiment in seconds.")
parser.add_argument("-u", "--units", default = "ns", type = str, help = "Units of input data. Enter 'ns' for nanoseconds and 'ms' for milliseconds.")
parser.add_argument("-c", "--columns", default = ["op_name", "request_id", "start_time", "end_time", "name_node_id"], nargs='+') # ["timestamp", "latency", "worker_id", "path"]
parser.add_argument("-o", "--output-path", dest = "output_path", default = None, type = str, help = "Output path to write graph to. If not specified, then no output will be saved.")

parser.add_argument("--show", action = 'store_true', help = "Show the plot rather than just write it to a file")
parser.add_argument("--legend", action = 'store_true', help = "Show the legend on each plot.")
parser.add_argument("--df", action = 'store_true', help = "Use dataframe-based algorithm to generate cost data (might be faster).")
parser.add_argument("--log", action = 'store_true', help = "Set the y-axis to log scale.")

parser.add_argument("--cpu", default = 5, type = float, help = "vCPU per NN.")
parser.add_argument("--memory", default = 19, type = float, help = "Memory per NN in GB.")

args = parser.parse_args()

input_path = args.input
label = args.label
duration = args.duration
units = args.units
COLUMNS = args.columns
output_path = args.output_path
show = args.show
cpu_per_nn = args.cpu
mem_per_nn = args.memory
use_df_alg = args.df
log_scale_y_axis = args.log

c2_standard_16_cost_per_second = 0.9406 / (60 * 60)
c2_standard_16_cost_per_ms = c2_standard_16_cost_per_second / 1000
cpu_cost_per_ms = 0.03827 / (60 * 60 * 1000) # Divide cost-per-hour by 60 min/hr * 60 sec/min * 1000 ms/sec.
mem_cost_per_ms = 0.00512 / (60 * 60 * 1000) # Divide cost-per-hour by 60 min/hr * 60 sec/min * 1000 ms/sec.
nn_cost_per_ms = (cpu_per_nn * cpu_cost_per_ms) + (mem_per_nn * mem_cost_per_ms)

print(COLUMNS)

# timestamp latency worker_id path
# COLUMNS = ['timestamp', 'latency', 'worker_id', 'path']

if units == 'ns':
    adjust_divisor = 1e9
elif units == 'ms':
    adjust_divisor = 1e3
else:
    raise ValueError("Unknown/unsupported units: " + str(units))

def compute_cost_of_operation(row):
    end_to_end_latency_ms = row["latency"]
    return (end_to_end_latency_ms * cpu_cost_per_ms) + (end_to_end_latency_ms * mem_cost_per_ms)

def compute_cost_of_latency(latency_ms):
    return (latency_ms * cpu_cost_per_ms) + (latency_ms * mem_cost_per_ms)

print("Reading DataFrame now...")

# If we pass a single .txt file, then just create DataFrame from the .txt file.
# Otherwise, merge all .txt files in the specified directory.
# if input_path.endswith(".txt") or input_path.endswith(".csv"):
#     df = pd.read_csv(input_path, index_col=None, header=0)
#     df.columns = COLUMNS
# INCOMPLETE:
# The 'else' branch below is incomplete, so I commented it out. 
# else:
#     if any(file == "ALL_OPS.txt" for file in os.listdir(input_path)):
#         all_ops_path = os.path.join(input_path, "ALL_OPS.txt")
#         df = pd.read_csv(all_ops_path, index_col=None, header=0)
#         df.columns = ["timestamp", "latency"] # Input columns.
        
#     else:
#         print("input_path: " + input_path)
#         print("joined: " + str(os.path.join(input_path, "*.txt")))
#         all_files = glob.glob(os.path.join(input_path, "*.txt"))
#         individual_dfs = []
#         print("Merging the following files: %s" % str(all_files))
#         # Merge the .txt files into a single DataFrame.
#         for filename in all_files:
#             print("Reading file: " + filename)
#             individual_df = pd.read_csv(filename, index_col=None, header=0)
#             individual_df.columns = ["timestamp", "latency"]
#             individual_dfs.append(individual_df)
#         df = pd.concat(individual_dfs, axis=0, ignore_index=True)
#         df.columns = ["timestamp", "latency"]
        
#         # Desired columns are: ["start_time", "end_time", "name_node_id"]
    
# Sort the DataFrame by timestamp.
# print("Done. Sorting now...")
# start_sort = time.time()
# df = df.sort_values('start_time')
# print("Sorted dataframe in %f seconds." % (time.time() - start_sort))
# min_val = min(df['start_time'])
# max_val = max(df['start_time'])
# print("max_val - min_val =", max_val - min_val)
# print("max_val - min_val =", (max_val - min_val) / adjust_divisor)
# def adjust(x):
#     return (x - min_val)# / adjust_divisor

# df['start_adjusted'] = df['start_time'].map(adjust)
# df['end_adjusted'] = df['end_time'].map(adjust)

# namenodes = df['name_node_id'].unique()

# experiment_end_time = df['end_adjusted'].max()

# if use_df_alg:
#     print("Building interval trees using DataFrame algorithm now...")

#     cost_at_each_ms_of_experiment = [0]
#     for current_ms in tqdm(range(0, experiment_end_time)):
#         # For each NN, add 1ms worth of cost if it had a task actively executing.
#         num_tasks_executing = df[(current_ms >= df['start_adjusted']) & (current_ms < df['end_adjusted'])].groupby('name_node_id').size().sum()
#         current_ms_cost = num_tasks_executing * nn_cost_per_ms
#         cost_at_each_ms_of_experiment.append(current_ms_cost + cost_at_each_ms_of_experiment[-1])

#     with open('df_cost.txt', 'w') as f:
#         for line in cost_at_each_ms_of_experiment:
#             f.write(f"{line}\n")
# else:
#     print("Building interval trees using Interval Tree algorithm now...")
#     from intervaltree import Interval, IntervalTree
#     interval_trees = dict()

#     for name_node_id in namenodes:
#         interval_trees[name_node_id] = IntervalTree()

#     for index, row in tqdm(df.iterrows(), total=df.shape[0]):
#         name_node_id = row['name_node_id']
#         start_time = row['start_adjusted']
#         end_time = row['end_adjusted']
#         data = row.to_dict()
#         interval_tree = interval_trees[name_node_id]
#         if start_time == end_time:
#             interval_tree[start_time:end_time+1] = data
#         else:
#             interval_tree[start_time:end_time] = data

#     cost_at_each_ms_of_experiment = [0]
#     for current_ms in tqdm(range(0, experiment_end_time)):
#         current_ms_cost = 0
#         # For each NN, add 1ms worth of cost if it had a task actively executing.
#         for nn_id, interval_tree in interval_trees.items():
#             if len(interval_tree[current_ms]) > 0:
#                 current_ms_cost += nn_cost_per_ms
#         cost_at_each_ms_of_experiment.append(current_ms_cost + cost_at_each_ms_of_experiment[-1])

#     with open('interval_tree_cost.txt', 'w') as f:
#         for line in cost_at_each_ms_of_experiment:
#             f.write(str(line))
#             f.write("\n")

with open(input_path, 'r') as f:
    cost_at_each_ms_of_experiment = [float(c) for c in f.readlines()]

nns_df = pd.read_csv("G:/Documents/School/College/MasonLeapLab_Research/ServerlessMDS/Benchmark/Spotify (MyApp)/10-14-2022/S1 (25k 12GB NNs)/nns.csv")
simple_cost = [0]
for i in range(1, 301):
    nns = nns_df['nns'][i]
    current_cost = simple_cost[i-1] + ((nns * 5 * cpu_cost_per_ms * 1000) + (nns * 12 * mem_cost_per_ms * 1000))
    simple_cost.append(current_cost)

#print("simple_cost:", simple_cost)

cost_fig, cost_axs = plt.subplots(nrows = 1, ncols = 1, figsize=(12,5))
hopsfs_cost = [0]
for i in range(0, len(cost_at_each_ms_of_experiment)):
    current_cost = hopsfs_cost[-1] + (32 * c2_standard_16_cost_per_ms)
    hopsfs_cost.append(current_cost)

print(cost_at_each_ms_of_experiment[-1])

simple_xs = [1000 * x for x in list(range(len(simple_cost)))[1:]]
cost_axs.plot(list(range(len(cost_at_each_ms_of_experiment)))[1:], cost_at_each_ms_of_experiment[1:], linewidth = 7, color = "#FF6B6B", label = r'$\lambda$' + "FS")
cost_axs.plot(simple_xs, simple_cost[1:], linewidth = 7, label = r'$\lambda$' + "FS (Simplified)", color = "#06D6A0")
cost_axs.plot(list(range(len(hopsfs_cost)))[1:], hopsfs_cost[1:], linewidth = 8, label = "HopsFS", color = "#7D8CC4")
cost_axs.plot(list(range(len(hopsfs_cost)))[1:], hopsfs_cost[1:], linewidth = 8, linestyle = (0, (2, 2)), label = "HopsFS+Cache", color = "#2E4057")



#cost_axs.set_ylim(bottom = 1e-4, top = 5)
cost_axs.set_xlabel("Time (milliseconds)", color = 'black')
if log_scale_y_axis:
    cost_axs.set_yscale('log')
    cost_fig.legend(loc = 'lower right', prop={'size': 40}, bbox_to_anchor=(0.0, 1), framealpha=0.0, handlelength=1, labelspacing=0.2, handletextpad = 0.4)
else:
    cost_fig.legend(loc='upper left', prop={'size': 22}, bbox_to_anchor=(0.1275, 0.93), framealpha=1, handlelength=1.75, labelspacing=0.1, handletextpad = 0.6)

cost_axs.set_ylabel("Cumulative Cost (USD)", color = 'black', fontsize = 25)
cost_axs.yaxis.set_label_coords(-0.0885, .40)
cost_axs.yaxis.set_major_locator(ticker.MultipleLocator(0.5))
cost_axs.xaxis.set_major_formatter(ticker.EngFormatter(sep=""))
print("Showing")

if args.show:
    cost_fig.show()

#cost_fig.show()
plt.tight_layout()
# if output_path is not None:
#   print("Saving plot to file '%s' now" % output_path)
plt.savefig("./spotify_cumulative_cost.pdf")
#   print("Done")
