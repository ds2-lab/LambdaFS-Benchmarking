import argparse
import numpy as np
import pandas as pd
import time
import random
import matplotlib as mpl
import matplotlib.pyplot as plt
import glob
import os
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset, inset_axes

# python latency_individual.py -xlim 0.25 -ylim 0.75 -ih "<hopsfs input path>" -il "<lambda-fs input path>" -n 25

#####################################################
# Latency comparison between HopsFS and \lambdaMDS. #
#####################################################
#
# Plots latencies INDIVIDUALLY for each operation.
#
# This version will compare HopsFS and \lambdaMDS data.
# The output files have different names, so this script
# accounts for that.

plt.style.use('ggplot')
mpl.rcParams['text.color'] = 'black'
mpl.rcParams['xtick.color'] = 'black'
mpl.rcParams['ytick.color'] = 'black'
mpl.rcParams["figure.figsize"] = (8,6)

font = {'weight' : 'bold',
        'size'   : 20}
mpl.rc('font', **font)

x_label_font_size = 20
y_label_font_size = 18
xtick_font_size = 18
markersize = 6
linewidth = 4

parser = argparse.ArgumentParser()

parser.add_argument("-i1", "--input1", dest="input1", help = "Path to file containing ALL data. Used for \lambdaMDS.")
parser.add_argument("-i2", "--input2", dest="input2", help = "Path to file containing ALL data. Used for HopsFS.")

parser.add_argument("-l1", "--label1", default = r'$\lambda$' + "MDS", help = "Label for first set of data.")
parser.add_argument("-l2", "--label2", default =  "HopsFS", help = "Label for second set of data.")

parser.add_argument("-ylim", default = 0.0, type = float, help = "Set the limit of each y-axis to this percent of the max value.")
parser.add_argument("-xlim", default = 1.0, type = float, help = "Set the limit of each x-axis to this percent of the max value.")

parser.add_argument("-n", default = 1, type = int, help = "Plot every `n` points (instead of all points).")

parser.add_argument("--show", action = 'store_true', help = "Show the plot rather than just write it to a file")
parser.add_argument("--legend", action = 'store_true', help = "Show the legend on each plot.")
parser.add_argument("--skip-plot", action = 'store_true', dest = 'skip_plot', help = "Don't actually plot any data.")

parser.add_argument("-o", "--output", type = str, default = None, help = "File path to write chart to. If none specified, then don't write to file.")
parser.add_argument("-c1", "--columns1", default = ["timestamp", "latency"], nargs='+')
parser.add_argument("-c2", "--columns2", default = ["timestamp", "latency"], nargs='+')

# ["timestamp", "latency", "worker_id", "path"]

args = parser.parse_args()

input1 = args.input1
input2 = args.input2
xlim_percent = args.xlim
ylim_percent = args.ylim
n = args.n
show_plot = args.show
output_path = args.output
show_legend = args.legend
label1 = args.label1
label2 = args.label2
columns1 = args.columns1
columns2 = args.columns2
skip_plot = args.skip_plot

input1_colors = ["#ffa822", "#124c6d", "#ff6150", "#1ac0c6", "#7c849c", "#6918b4", "#117e16", "#ff7c00", "#ff00c5"]
input2_colors = ["#cc7a00", "#0b2e42", "#b31200", "#128387", "#424757", "#410f70", "#0c5a10", "#b35600", "#cc009c"]
ops = {}
sub_axis = {}
next_idx = 0

name_mapping = {
    "delete": "DELETE",
    "getListing": "LS DIR",
    "getFileInfo": "STAT",
    "mkdirs": "MKDIR",
    "getBlockLocations": "READ",
    "rename": "RENAME"
}

averages_1 = {}
averages_2 = {}

counts_1 = {}
counts_2 = {}

df_s_create = None
df_s_complete = None

def plot_data(input_path, columns = ["timestamp", "latency"], axis = None, dataset = 1, label = ""):
    global ops
    global next_idx
    global df_s_complete
    global df_s_create

    num_cold_starts = 0

    # If we pass a single .txt file, then just create DataFrame from the .txt file.
    # Otherwise, merge all .txt files in the specified directory.
    if input_path.endswith(".txt"):
        df = pd.read_csv(input_path)
    else:
        all_files = glob.glob(os.path.join(input_path, "*.txt"))

        if dataset == 1:
            colors = input1_colors
            marker = "X"
            markersize = 8
        else:
            colors = input2_colors
            marker = "*"
            markersize = 10

        idx = 0

        # Merge the .txt files into a single DataFrame.
        for i, filename in enumerate(all_files):
            print("Reading file: " + filename)
            num_starts_for_op = 0
            df = pd.read_csv(filename, index_col=None, header=0, )
            df.columns = columns

            # Sort the DataFrame by timestamp.
            df = df.sort_values('latency')
            # df['latency'] = df['latency'].map(lambda x: x / 1.0e6)

            latencies = df['latency'].values.tolist()

#             while (latencies[-1] > 3000):
#               latencies = latencies[:-1]
#               num_cold_starts += 1
#               num_starts_for_op += 1

            fs_operation_name = os.path.basename(filename)[:-4] # remove the ".txt" with `[:-4]`

            if (dataset == 1):
                if (fs_operation_name in name_mapping):
                    fs_operation_name = name_mapping[fs_operation_name]
                elif fs_operation_name == "complete":
                    df_s_complete = df

                    if (df_s_create is not None):
                        df_s_create['latency'] = df_s_create['latency'] + df_s_complete['latency']
                        df = df_s_create
                        fs_operation_name = "CREATE FILE"
                    else:
                        continue
                elif fs_operation_name == "create":
                    df_s_create = df

                    if (df_s_complete is not None):
                        df_s_create['latency'] = df_s_create['latency'] + df_s_complete['latency']
                        df = df_s_create
                        fs_operation_name = "CREATE FILE"
                    else:
                        continue

            current_label = "%s - %s" % (label, fs_operation_name)

            if dataset == 1:
                averages_1[fs_operation_name] = np.mean(latencies)
                counts_1[fs_operation_name] = len(latencies)
                #print("Average latency for %s: %f ms" % (current_label, averages_1[fs_operation_name]))
            else:
                averages_2[fs_operation_name] = np.mean(latencies)
                counts_2[fs_operation_name] = len(latencies)
                #print("Average latency for %s: %f ms" % (current_label, averages_2[fs_operation_name]))

            print("Removed %d points for %s" % (num_starts_for_op, current_label))

            if skip_plot:
                continue

            print("fs_operation_name: " + fs_operation_name)
            if fs_operation_name in ops:
              print("Found " + fs_operation_name + " in ops")
              idx = ops[fs_operation_name]
            else:
              print("Did NOT find " + fs_operation_name + " in ops")
              idx = next_idx
              next_idx += 1
              ops[fs_operation_name] = idx

            if fs_operation_name in sub_axis:
                axins = sub_axis[fs_operation_name]
            else:
                axins = inset_axes(axis[idx], 2, 2, bbox_transform=axis[idx].transAxes, bbox_to_anchor=(0.90, 0.85))
                axins.set_xlim(left = -10, right = min(latencies[-1] * 0.25, 300))
                axins.set_ylim(bottom = 0.95, top = 1.01)
                sub_axis[fs_operation_name] = axins

            print("max(latencies): ", max(latencies))

            ys = list(range(0, len(latencies)))
            ys = [y / len(ys) for y in ys]

            axis[idx].plot(latencies[::n] + [latencies[-1]], ys[::n] + [ys[-1]], label = label, linewidth = 2, markersize = markersize, marker = marker, markevery = 0.1, color = colors[idx])
            #axis[idx].plot(latencies, ys, label = current_label, linewidth = 2, markersize = markersize, marker = marker, markevery = 0.1, color = colors[idx])
            axis[idx].set_yscale('linear')
            axis[idx].set_xlabel("Latency (ms)", fontsize = x_label_font_size)
            axis[idx].set_ylabel("Cumulative Probability", fontsize = y_label_font_size)
            axis[idx].tick_params(labelsize=xtick_font_size)
            axis[idx].set_title(fs_operation_name)
            #axis[idx].set_xlim(left = -1, right = 250) #(xlim_percent * latencies[-1]) * 1.05)
            axis[idx].set_ylim(bottom = ylim_percent, top = 1.0125)
            axis[idx].xaxis.label.set_color('black')
            axis[idx].yaxis.label.set_color('black')

            axins.plot(latencies[::n] + [latencies[-1]], ys[::n] + [ys[-1]], label = label, linewidth = 1.85, markersize = markersize * 0.675, marker = marker, markevery = 0.15, color = colors[idx])

    print("Removed a total of %d points." % num_cold_starts)

if input1 is not None:
    num_input1_files = len(glob.glob(os.path.join(input1, "*.txt")))
else:
    num_input1_files = 0

if input2 is not None:
    num_input2_files = len(glob.glob(os.path.join(input2, "*.txt")))
else:
    num_input2_files = 0

num_columns = 7 #max(num_input1_files, num_input2_files)

print("Plotting data now...")

fig, axs = plt.subplots(nrows = 1, ncols = num_columns, figsize=(40, 6))
plot_start = time.time()
if input1 is not None:
    plot_data(input1, axis = axs, dataset = 1, label = label1, columns = columns1)
if input2 is not None:
    plot_data(input2, axis = axs, dataset = 2, label = label2, columns = columns2)

print("Done. Plotted all data points in %f seconds." % (time.time() - plot_start))

total_1 = 0
for val in counts_1.values():
    total_1 += val

total_2 = 0
for val in counts_2.values():
    total_2 += val

print("\n\nCounts for %s:" % label1)
for op, num in counts_1.items():
    print("%s: %d/%d (%.2f)" % (op, num, total_1, (num/total_1) * 100))
print("\nCounts for %s:" % label2)
for op, num in counts_2.items():
    print("%s: %d/%d (%.2f)" % (op, num, total_2, (num/total_2) * 100))

print("\n\nAverages for %s:" % label1)
for op, avg in averages_1.items():
    print("%s: %f" % (op, avg))
print("\nAverages for %s:" % label2)
for op, avg in averages_2.items():
    print("%s: %f" % (op, avg))

if skip_plot:
    exit(0)

if show_legend:
    for ax in axs:
        ax.legend(loc = 'lower right')

#fig.legend()
#plt.suptitle("Latency CDF - Spotify Workload - Log Scale x-Axis")
# axs.set_yscale('linear')
# axs.set_xlabel("Latency (ms)", fontsize = x_label_font_size)
# axs.set_ylabel("Cumulative Probability", fontsize = y_label_font_size)
# axs.tick_params(labelsize=xtick_font_size)
# axs.set_title("CDF - Spotify Workload - Log Scale x-Axis")

axs.xaxis.set_major_formatter(ticker.EngFormatter(sep=""))

plt.tight_layout()

if output_path is not None:
  print("Saving plot to file '%s' now" % output_path)
  plt.savefig(output_path)
  print("Done")

if show_plot:
  print("Displaying figure now.")
  plt.show()