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
import pickle 
import yaml 
from ast import literal_eval as make_tuple
import re

from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset

# # # # # # # # # # # # # # # # # #
# Plot throughput for \lambdaMDS. #
# # # # # # # # # # # # # # # # # #

plt.style.use('ggplot')
mpl.rcParams['text.color'] = 'black'
mpl.rcParams['xtick.color'] = 'black'
mpl.rcParams['ytick.color'] = 'black'
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
#mpl.rcParams["figure.figsize"] = (8,6)

font = {'weight' : 'bold',
        'size'   : 40}
mpl.rc('font', **font)

parser = argparse.ArgumentParser()

parser.add_argument("-i", "--input", type = str, default = None, help = "Path to input file. Each line contains a pair of the form \"<path>;<label>\", specifying an input and its associated label. This is an alternative to passing specific inputs via the -i1 -l1 -i2 -l2 -i3 -l3 flags.")

parser.add_argument("-n", "--namenodes", default = None, help = "Path to associated NN monitoring CSV.")
parser.add_argument("-d", "--duration", default = 60, type = int, help = "Duration of the experiment in seconds.")
parser.add_argument("-u", "--units", default = "ns", type = str, help = "Units of input data. Enter 'ns' for nanoseconds and 'ms' for milliseconds.")
parser.add_argument("-c", "--columns", default = ["timestamp", "latency"], nargs='+') # ["timestamp", "latency", "worker_id", "path"]
parser.add_argument("-o", "--output-path", dest = "output_path", default = None, type = str, help = "Output path to write graph to. If not specified, then no output will be saved.")
parser.add_argument("--show", action = 'store_true', help = "Show the plot rather than just write it to a file")
parser.add_argument("--legend", action = 'store_true', help = "Show the legend on each plot.")
#parser.add_argument("--cost", action = 'store_true', help = "Show the legend on each plot.")
parser.add_argument("--no-y-axis-labels", dest = "no_y_axis_labels", action = 'store_true', help = "Do not plot y-axis labels.")

parser.add_argument("--cpu", default = 5, type = float, help = "vCPU per NN.")
parser.add_argument("--memory", default = 19, type = float, help = "Memory per NN in GB.")

args = parser.parse_args()

input_file_path = args.input

duration = args.duration
namenodes_path = args.namenodes
units = args.units
COLUMNS = args.columns
output_path = args.output_path
show = args.show
cpu_per_nn = args.cpu
mem_per_nn = args.memory
#plot_cost = args.cost
no_y_axis_labels = args.no_y_axis_labels

c2_standard_16_cost_per_second = 0.9406 / (60 * 60)
cpu_cost_per_ms = 0.03827 / (60 * 60 * 1000) # Divide cost-per-hour by 60 min/hr * 60 sec/min * 1000 ms/sec.
mem_cost_per_ms = 0.00512 / (60 * 60 * 1000) # Divide cost-per-hour by 60 min/hr * 60 sec/min * 1000 ms/sec.

print(COLUMNS)

COLORS  = ['#E24A33', '#348ABD', '#048513', '#6b078a', '#c97200', '#0eede9', '#ff2b6b']
MARKERS = ['x', '.', 'X', 'o', 'v', '^', '<', '>']
marker_idx = 0
color_idx = 0

# timestamp latency worker_id path
# COLUMNS = ['timestamp', 'latency', 'worker_id', 'path']

if units == 'ns':
    adjust_divisor = 1e9
elif units == 'ms':
    adjust_divisor = 1e3
else:
    raise ValueError("Unknown/unsupported units: " + str(units))

assert(input_file_path is not None)

# if namenodes_path is not None:
#     fig, axs = plt.subplots(nrows = 1, ncols = 2, figsize=(12,8))
# else:
#     fig, axs = plt.subplots(nrows = 1, ncols = 1, figsize=(12,8))

#if plot_cost:
#    cost_fig, cost_axs = plt.subplots(nrows = 1, ncols = 1, figsize=(12,10))
fig, axs = plt.subplots(nrows = 1, ncols = 1, figsize=(12,10))
axs.set_xlabel("Time (seconds)", color = 'black')
if not no_y_axis_labels:
    axs.set_ylabel("Performance per Cost", color = 'black')
axs.yaxis.set_major_formatter(ticker.EngFormatter(sep=""))

def compute_cost_of_operation(row):
    end_to_end_latency_ms = row["latency"]
    return (end_to_end_latency_ms * cpu_cost_per_ms) + (end_to_end_latency_ms * mem_cost_per_ms)

def plot(input:dict):
    global color_idx
    global marker_idx
    
    input_path = input["path"]
    label = input.get("label", "No-Label-Specified")

    if "L-MDS" in label:
        label = label.replace("L-MDS", r'$\lambda$' + "FS")

    # Adding the 'or' part ensures that, if an empty value is specified in the yaml (i.e., "markersize: " with no number), then we still default to a valid value.
    marker = input.get("marker", "None") or "None"
    markersize = input.get("markersize", 8) or 8
    linestyle = input.get("linestyle", "solid") or "solid"
    linewidth = input.get("linewidth", 4) or 4
    markevery = input.get("markevery", 0.1)
    # secondary_label = input.get("secondarylabel", None)
    secondary_path = input.get("secondarypath", None) 
    buckets_path = input.get("buckets-path", None)
        
    # if secondary_path is not None and secondary_axis is None:
    #     print("Creating secondary axis!")
    #     secondary_axis = axs.twinx()
    #     secondary_axis.set_ylabel("Perf. per Cost")
    
    if "linecolor" in input:
        linecolor = input["linecolor"]
        
        # If the color is specified as 6-character hex, then prepend it with a '#' 
        if len(linecolor) == 6 and re.search(r'^#(?:[0-9a-fA-F]{3}){1,2}$', "#" + linecolor):
            linecolor = "#" + linecolor
    else:
        linecolor = 'black'
    
    # The user may have specified something like (10, (10, 10)), which is a format matplotlib interprets to add offset and whatnot to the dashes. 
    if type(linestyle) is str:
        try:
            linestyle = make_tuple(linestyle)
            print("Setting linestyle to tuple: %s (type is %s)" % (str(linestyle), type(linestyle)))
        except:
            pass 
    
    print("Marker: %s\nMarker Size: %s\nLine style: %s\nLine color: %s\nLine width: %s" % (str(marker), str(markersize), linestyle, linecolor, str(linewidth)))
    
    start_time = time.time()
    
    if buckets_path is None:
        # If we pass a single .txt file, then just create DataFrame from the .txt file.
        # Otherwise, merge all .txt files in the specified directory.
        if input_path.endswith(".txt") or input_path.endswith(".csv"):
            print("Reading existing DF from file at '%s'" % input_path)
            df = pd.read_csv(input_path, index_col=None, header=0)
            print("Existing DF has the following columns: %s" % str(df.columns))
            if len(df.columns) == 2:
                df.columns = COLUMNS
                print("Set DF's columns to %s" % str(COLUMNS))
            
            print("Loaded existing DF in %f seconds" % (time.time() - start_time))
        else:
            print("input_path: " + input_path)
            print("joined: " + str(os.path.join(input_path, "*.txt")))
            all_files = glob.glob(os.path.join(input_path, "*.txt"))
            li = []
            print("Merging the following files: %s" % str(all_files))
            # Merge the .txt files into a single DataFrame.
            for filename in all_files:
                print("Reading file: " + filename)
                tmp_df = pd.read_csv(filename, index_col=None, header=0)
                tmp_df.columns = COLUMNS
                li.append(tmp_df)
            df = pd.concat(li, axis=0, ignore_index=True)
            df.columns = COLUMNS
            
            print("Loaded data and created DF in %f seconds" % (time.time() - start_time))

        st_time = time.time()
        if 'ts' not in df.columns:
            # Sort the DataFrame by timestamp.
            print("Sorting DF and creating `ts` column now...")
            start_sort = time.time()
            df = df.sort_values('timestamp')
            print("Sorted dataframe in %f seconds." % (time.time() - start_sort))

            min_val = min(df['timestamp'])
            max_val = max(df['timestamp'])
            print("max_val - min_val =", max_val - min_val)
            print("max_val - min_val =", (max_val - min_val) / adjust_divisor)
            def adjust(x):
                return (x - min_val) / adjust_divisor

            # Sometimes, there's a bunch of data with WAY different timestamps -- like, several THOUSAND
            # seconds different. So, I basically adjust all of that data so it fits within the interval
            # of the rest of the data.
            df['ts'] = df['timestamp'].map(adjust)
            df2 = df[((df['ts'] >= duration+5))]
            if len(df2) > 0:
                min_val2 = min(df2['ts'])

                def adjust2(x):
                    if x >= min_val2:
                        return x - min_val2
                    return x

                df['ts'] = df['ts'].map(adjust2)

            #df.to_csv("df" + str(dataset) + ".csv")
            
            print("Added `ts` column to DF in %f seconds" % (time.time() - st_time))
        print(df)

        print("Total number of points: %d" % len(df))
        #df.to_csv("./%s-cost-df.csv" % label)
        print("Done.")
        st_time = time.time()
        cumulative_cost = [0]
    
        print("Creating buckets now...")
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

        print("Sum of buckets: %d" % total)
        print("Average Throughput: " + str(np.mean(buckets)) + " ops/sec.")
        print("Average Latency: " + str(df['ts'].mean()) + " ms.")
        print("Computed cost for all buckets in %f seconds" % (time.time() - st_time))
        
        with open('%s_buckets.pkl' % label, 'wb') as bucket_file:
            pickle.dump(buckets, bucket_file)
            print("Wrote 'buckets' file for %s to file at ./%s_buckets.pkl" % (label, label))
    else:
        print("Loading buckets from file at '%s'" % buckets_path)
        with open(buckets_path, "rb") as bucket_file:
            buckets = pickle.load(bucket_file)
    
    start_time = time.time()

    if secondary_path is not None:
        print("Plotting secondary dataset.")
        secondary_df = pd.read_csv(secondary_path)
        print("Loaded secondary DF for %s in %f seconds." % (label, time.time() - start_time))
        start_time = time.time() 
        ys = secondary_df["nns"].values
        
        metric_values = []
        cost_factor_vcpu = 0.03827 / 3600 # Per second
        cost_factor_mem  = 0.00512 / 3600 # Per second
        for i in range(len(ys)):
            instantaneous_throughput = buckets[i]
            instantaneous_cost = (ys[i] * cost_factor_vcpu * 5) + (ys[i] * cost_factor_mem * 12)
            metric = instantaneous_throughput / instantaneous_cost
            metric_values.append(metric)
        
        start_time = time.time() 
        print("Computed costs for %s in %f seconds" % (label, time.time() - start_time))
        start_time = time.time() 
        #axs.plot(list(range(len(buckets))), buckets, label = label + " Throughput", linestyle = linestyle, linewidth = linewidth, marker = marker, markevery=markevery, markersize = markersize, color = linecolor) 
        axs.plot(list(range(len(metric_values))), metric_values, label = label, linestyle = linestyle, linewidth = linewidth, marker = marker, markevery=markevery, markersize = markersize, color = linecolor) 
        
        print("Plotting series %s in %f seconds" % (label, time.time() - start_time))
    else:
        cost_factor = (16 * 0.03827 * 32) / 3600
        cost_factor += (19 * 0.00512 * 32) / 3600
        #axs.plot(list(range(len(buckets))), buckets, label = label + " Throughput", linestyle = linestyle, linewidth = linewidth, marker = marker, markevery=markevery, markersize = markersize, color = linecolor) 
        axs.plot(list(range(len(buckets))), [b / cost_factor for b in buckets], label = label, linestyle = linestyle, linewidth = linewidth, marker = marker, markevery=markevery, markersize = markersize, color = linecolor) 
        
        print("Plotting series %s in %f seconds" % (label, time.time() - start_time))
        
    # if plot_cost:
    #     cost_axs.plot(list(range(len(cumulative_cost))), cumulative_cost, linewidth = 4, color = '#E24A33', label = r'$\lambda$' + "MDS")
    #     hopsfs_cost = [0]
    #     print("len(cumulative_cost): %d" % len(cumulative_cost))
    #     for i in range(0, len(cumulative_cost)):
    #         current_cost = hopsfs_cost[-1] + (32 * c2_standard_16_cost_per_second)
    #         hopsfs_cost.append(current_cost)
    #     cost_axs.plot(list(range(len(hopsfs_cost))), hopsfs_cost, linewidth = 4, color = '#348ABD', label = "HopsFS")

with open(input_file_path, 'r') as input_file:
    inputs = yaml.safe_load(input_file)

for i, input in enumerate(inputs):
    print("\n\n\nPlotting dataset #%d: '%s'. Path: '%s'" % (i, input["label"], input["path"]))
    plot(input)

axs.tick_params(axis='x', labelsize=40)
axs.tick_params(axis='y', labelsize=40)

if args.legend:
    lines = []
    labels = []

    for ax in fig.axes:
        Line, Label = ax.get_legend_handles_labels()
        print("Label: '%s'" % str(Label))
        
        if len(Label) == 0:
            continue
        
        if Label[0] not in labels:
            lines.extend(Line)
            labels.extend(Label)

    lines, labels = axs.get_legend_handles_labels()
    ax.legend(lines, labels, loc='upper left', prop={'size': 40}, bbox_to_anchor=(0.0, 1), framealpha=0.0, handlelength=1, labelspacing=0.2)

    # fig.legend(lines, labels, loc='upper left', prop={'size': 40}, bbox_to_anchor=(0.21, 0.975), framealpha=0.0, handlelength=1, labelspacing=0.2)

# if plot_cost:
#     cost_axs.set_xlabel("Time (seconds)", color = 'black')
#     cost_axs.set_ylabel("Cumulative Cost (USD)", color = 'black')
#     cost_fig.legend(loc = 'upper left', bbox_to_anchor=(0.16, 0.85))

plt.tight_layout()

if output_path is not None:
  print("Saving plot to file '%s' now" % output_path)
  plt.savefig(output_path)
  print("Done")

if args.show:
    plt.show()

    # if plot_cost:
    #     cost_fig.show()