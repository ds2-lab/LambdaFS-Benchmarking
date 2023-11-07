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

parser.add_argument("-i1", "--input1", default = "./ALL_DATA.txt", help = "Path to folder containing the data.")
parser.add_argument("-i2", "--input2", default = None, help = "Path to folder containing the data.")
parser.add_argument("-i3", "--input3", default = None, help = "Path to folder containing the data.")
parser.add_argument("-l1", "--label1", default = r'$\lambda$' + "MDS", help = "Label for first set of data.")
parser.add_argument("-l2", "--label2", default = "HopsFS", help = "Label for second set of data.")
parser.add_argument("-l3", "--label3", default = r'$\lambda$' + "MDS Small Cache", help = "Label for second set of data.")

parser.add_argument("-i", "--input", type = str, default = None, help = "Path to input file. Each line contains a pair of the form \"<path>;<label>\", specifying an input and its associated label. This is an alternative to passing specific inputs via the -i1 -l1 -i2 -l2 -i3 -l3 flags.")

parser.add_argument("-n", "--namenodes", default = None, help = "Path to associated NN monitoring CSV.")
parser.add_argument("-d", "--duration", default = 60, type = int, help = "Duration of the experiment in seconds.")
parser.add_argument("-u", "--units", default = "ns", type = str, help = "Units of input data. Enter 'ns' for nanoseconds and 'ms' for milliseconds.")
parser.add_argument("-c", "--columns", default = ["timestamp", "latency"], nargs='+') # ["timestamp", "latency", "worker_id", "path"]
parser.add_argument("-o", "--output-path", dest = "output_path", default = None, type = str, help = "Output path to write graph to. If not specified, then no output will be saved.")
parser.add_argument("--show", action = 'store_true', help = "Show the plot rather than just write it to a file")
parser.add_argument("--legend", action = 'store_true', help = "Show the legend on each plot.")
parser.add_argument("--cost", action = 'store_true', help = "Show the legend on each plot.")
parser.add_argument("--no-y-axis-labels", dest = "no_y_axis_labels", action = 'store_true', help = "Do not plot y-axis labels.")

parser.add_argument("--cpu", default = 5, type = float, help = "vCPU per NN.")
parser.add_argument("--memory", default = 19, type = float, help = "Memory per NN in GB.")

args = parser.parse_args()

input_path1 = args.input1
input_path2 = args.input2
input_path3 = args.input3
label1 = args.label1
label2 = args.label2
label3 = args.label3

input_file_path = args.input

duration = args.duration
namenodes_path = args.namenodes
units = args.units
COLUMNS = args.columns
output_path = args.output_path
show = args.show
cpu_per_nn = args.cpu
mem_per_nn = args.memory
plot_cost = args.cost
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

# Check for a nns.csv in the input path.
if namenodes_path is None:
    namenodes_path = input_path1 + "/nns.csv"
    if not os.path.isfile(namenodes_path):
        namenodes_path = None

# if namenodes_path is not None:
#     fig, axs = plt.subplots(nrows = 1, ncols = 2, figsize=(12,8))
# else:
#     fig, axs = plt.subplots(nrows = 1, ncols = 1, figsize=(12,8))

if plot_cost:
    cost_fig, cost_axs = plt.subplots(nrows = 1, ncols = 1, figsize=(12,10))
fig, axs = plt.subplots(nrows = 1, ncols = 1, figsize=(12,10))
axs.set_xlabel("Time (seconds)", color = 'black')
if not no_y_axis_labels:
    axs.set_ylabel("Throughput (ops/sec)", color = 'black')
axs.yaxis.set_major_formatter(ticker.EngFormatter(sep=""))

if namenodes_path is not None:
    nns_axs = axs.twinx()
else:
    nns_axs = None

def compute_cost_of_operation(row):
    end_to_end_latency_ms = row["latency"]
    return (end_to_end_latency_ms * cpu_cost_per_ms) + (end_to_end_latency_ms * mem_cost_per_ms)

def plot(input_path, df = None, label = None, dataset = -1):
    global color_idx
    global marker_idx
    # If we pass a single .txt file, then just create DataFrame from the .txt file.
    # Otherwise, merge all .txt files in the specified directory.
    if df is None:
        if input_path.endswith(".txt") or input_path.endswith(".csv"):
            df = pd.read_csv(input_path, index_col=None, header=0)
            print("Read DF")
            #df.columns = COLUMNS
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

    if 'ts' not in df.columns:
        # Sort the DataFrame by timestamp.
        print("Sorting now...")
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
    print(df)

    print("Total number of points: %d" % len(df))
    if plot_cost and 'cost' not in df.columns:
        print("Computing cost column now...")
        df['cost'] = df.apply(lambda row: compute_cost_of_operation(row), axis = 1)
        df.to_csv("./nns.csv")
    print("Done.")
    cumulative_cost = [0]

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

        if plot_cost:
            current_cost = res['cost'].values[::10].sum()
            last_cost = cumulative_cost[-1]
            cumulative_cost.append(last_cost + current_cost)

    print("Sum of buckets: %d" % total)

    print("Average Throughput: " + str(np.mean(buckets)) + " ops/sec.")
    print("Average Latency: " + str(df['ts'].mean()) + " ms.")

    if namenodes_path is not None:
        if (dataset == 1):
            df_nns = pd.read_csv(namenodes_path)
            min_val = min(df_nns["time"])

            def adjust_nn_timestamp(timestamp):
                return (timestamp - min_val) / 1e3

            df_nns["ts"] = df_nns["time"].map(adjust_nn_timestamp)

            # Adjust to account for the warm-up.
            t = 12
            xs = df_nns["ts"].values
            ys = df_nns["nns"].values
            ys = [y for y in ys]
            ys = ys[0:t] + ys[t+t:]

            xs = xs[0:300]
            ys = ys[0:300]

            for i in range(0, len(xs)):
                if xs[i] > 300:
                    xs[i] = 300

            axs.plot(list(range(len(buckets))), buckets, label = label, linewidth = 4, color = COLORS[color_idx]) #color = '#E24A33')
            # axs.set_xlim(left = -5, right = 305)

            if plot_cost:
                cost_axs.plot(list(range(len(cumulative_cost))), cumulative_cost, linewidth = 4, color = '#E24A33', label = r'$\lambda$' + "MDS")
                hopsfs_cost = [0]
                print("len(cumulative_cost): %d" % len(cumulative_cost))
                for i in range(0, len(cumulative_cost)):
                    current_cost = hopsfs_cost[-1] + (32 * c2_standard_16_cost_per_second)
                    hopsfs_cost.append(current_cost)
                cost_axs.plot(list(range(len(hopsfs_cost))), hopsfs_cost, linewidth = 4, color = '#348ABD', label = "HopsFS")

            #axs.set_ylim(bottom = 0, top = 170000)
            axs.yaxis.set_major_locator(ticker.MultipleLocator(50_000))

            if nns_axs is not None:
                nns_axs.plot(xs, ys, color = 'grey', linewidth = 4, linestyle='dashed', label = r'$\lambda$' + "MDS NNs")

            if not no_y_axis_labels and nns_axs is not None:
                nns_axs.set_ylabel("Active " + r'$\lambda$' + "MDS NNs", color = 'black')
        #elif (dataset == 2):
        #    axs.plot(list(range(len(buckets))), buckets, label = label, linewidth = 4, marker = 'x', markevery=0.025, markersize = 8, color = COLORS[color_idx]) #'#0065a1')
        #elif (dataset == 3):
        #    axs.plot(list(range(len(buckets))), buckets, label = label, linewidth = 4, marker = '.', markevery=0.025, markersize = 14, color = COLORS[color_idx]) #'#6b1204')
        else:
            axs.plot(list(range(len(buckets))), buckets, label = label, linewidth = 4, marker = MARKERS[marker_idx], markevery=0.025, markersize = 11, color = COLORS[color_idx]) #'#6b1204')
        
        color_idx += 1
        marker_idx += 1
        #plt.tight_layout()
    else:
        axs.plot(list(range(len(buckets))), buckets, label = label, linewidth = 4, markersize = 14)

        if plot_cost:
            cumulative_cost_est = [c * 0.75 for c in cumulative_cost]
            cost_axs.plot(list(range(len(cumulative_cost_est))), cumulative_cost_est, linewidth = 4, color = '#E24A33', label = r'$\lambda$' + "MDS")
            hopsfs_cost = [0]
            print("len(cumulative_cost): %d" % len(cumulative_cost))

if input_file_path is not None:
    inputs, labels = [], []
    
    # Parse the input file to extract the labels and the associated paths.
    with open(input_file_path, 'r') as input_file:
        path_label_pairs = input_file.readlines()
        
        for line in path_label_pairs:
            # Comment 
            if line[0] == "#":
                continue 
            
            tmp = line.split(";")
            input = tmp[0]
            label = tmp[1].strip()
            
            print("Discovered input '%s' with path '%s'" % (label, input))
            
            inputs.append(input)
            labels.append(label)
    
    for i, tmp in enumerate(list(zip(labels, inputs))):
        label, input_path = tmp
        print("\n\n\nPlotting dataset #%d: '%s'. Path: '%s'" % (i, label, input_path))
        plot(input_path, label = label, dataset = i)
    
else:
    if input_path1 is not None:
        print("Plotting dataset1 %s: '%s'" % (label1, input_path1))
        plot(input_path1, label = label1, dataset = 1)

    if input_path2 is not None:
        print("Plotting dataset2 %s: '%s'" % (label2, input_path2))
        plot(input_path2, label = label2, dataset = 2)

    if input_path3 is not None:
        print("Plotting dataset3 %s: '%s'" % (label3, input_path3))
        plot(input_path3, label = label3, dataset = 3)

axs.tick_params(axis='x', labelsize=40)
axs.tick_params(axis='y', labelsize=40)
try:
    if nns_axs is not None:
        nns_axs.tick_params(axis='y', labelsize=40)
except:
    print("[ERROR] No axs2 exists...")
    pass

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

    fig.legend(lines, labels, loc='upper left', prop={'size': 40}, bbox_to_anchor=(0.21, 0.975), framealpha=0.0, handlelength=1, labelspacing=0.2)

if plot_cost:
    cost_axs.set_xlabel("Time (seconds)", color = 'black')
    cost_axs.set_ylabel("Cumulative Cost (USD)", color = 'black')
    cost_fig.legend(loc = 'upper left', bbox_to_anchor=(0.16, 0.85))

plt.tight_layout()

if output_path is not None:
  print("Saving plot to file '%s' now" % output_path)
  plt.savefig(output_path)
  print("Done")

if args.show:
    plt.show()

    if plot_cost:
        cost_fig.show()