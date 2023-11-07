import argparse
import numpy as np
import pandas as pd
import time
import random
import matplotlib as mpl
import matplotlib.pyplot as plt
import glob
import os

# python latency_merged.py -xlim 0.25 -ylim 0.75 -ih "<hopsfs input path>" -il "<lambda-fs input path>" -n 25

#####################################################
# Latency comparison between HopsFS and \lambdaMDS. #
#####################################################
#
# This version will compare HopsFS and \lambdaMDS data.
# The output files have different names, so this script
# accounts for that.

from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset

plt.style.use('ggplot')
mpl.rcParams['text.color'] = 'black'
mpl.rcParams['xtick.color'] = 'black'
mpl.rcParams['ytick.color'] = 'black'
mpl.rcParams["figure.figsize"] = (8,6)
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42

font = {'weight' : 'bold',
        'size'   : 32}
mpl.rc('font', **font)

x_label_font_size = 32
y_label_font_size = 32
xtick_font_size = 32
markersize = 10
linewidth = 4

parser = argparse.ArgumentParser()

parser.add_argument("-ih", "--input-hopsfs", dest="input_hopsfs", help = "Path to file containing ALL data.")
parser.add_argument("-il", "--input-lambdamds", dest="input_lambdamds", help = "Path to file containing ALL data.")

parser.add_argument("-ylim", default = 0.5, type = float, help = "Set the limit of each y-axis to this percent of the max value.")
parser.add_argument("-xlim", default = 1.0, type = float, help = "Set the limit of each x-axis to this percent of the max value.")

parser.add_argument("-n", default = 1, type = int, help = "Plot every `n` points (instead of all points).")

parser.add_argument("-o", "--output", type = str, default = None, help = "File path to write chart to. If none specified, then don't write to file.")
parser.add_argument("-c", "--columns", default = ["timestamp", "latency", "worker_id", "path"], nargs='+')

parser.add_argument("--show", action = 'store_true', help = "Show the plot rather than just write it to a file")
parser.add_argument("--legend", action = 'store_true', help = "Show the legend on each plot.")
parser.add_argument("--skip-plot", action = 'store_true', dest = 'skip_plot', help = "Don't actually plot any data.")

parser.add_argument("-l1", "--label1", default = r'$\lambda$' + "MDS", help = "Label for first set of data.")
parser.add_argument("-l2", "--label2", default =  "HopsFS", help = "Label for second set of data.")

args = parser.parse_args()

input_hopsfs = args.input_hopsfs
input_lambdamds = args.input_lambdamds
label1 = args.label1
label2 = args.label2
xlim_percent = args.xlim
ylim_percent = args.ylim
n = args.n
show_plot = args.show
output_path = args.output
COLUMNS = args.columns
show_legend = args.legend
skip_plot = args.skip_plot

print(COLUMNS)

input1_colors = ["#ffa822", "#124c6d", "#ff6150", "#1ac0c6", "#7c849c", "#6918b4", "#117e16", "#ff7c00", "#ff00c5"]
input2_colors = ["#cc7a00", "#0b2e42", "#b31200", "#128387", "#424757", "#410f70", "#0c5a10", "#b35600", "#cc009c"]

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

op_color_indices = {}
idx = 0

def plot_data(input_path, columns = ["timestamp", "latency"], axis = None, dataset = 1, label = ""):
    global ops
    global next_idx
    global df_s_complete
    global df_s_create
    global idx

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

            fs_operation_name = os.path.basename(filename)[:-4] # remove the ".txt" with `[:-4]`
            current_label = "%s %s" % (label, fs_operation_name)

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

            current_label = "%s %s" % (label, fs_operation_name)

            if fs_operation_name not in op_color_indices:
                print("Could not find %s in colors." % fs_operation_name)
                op_color_indices[fs_operation_name] = idx
                idx += 1

            if dataset == 1:
                averages_1[fs_operation_name] = np.mean(latencies)
                counts_1[fs_operation_name] = len(latencies)
                line_color = input1_colors[op_color_indices[fs_operation_name]]
            else:
                averages_2[fs_operation_name] = np.mean(latencies)
                counts_2[fs_operation_name] = len(latencies)
                line_color = input2_colors[op_color_indices[fs_operation_name]]

            if skip_plot:
                continue

            print("max(latencies) for " + fs_operation_name + ": ", max(latencies))

            ys = list(range(0, len(latencies)))
            ys = [y / len(ys) for y in ys]

            axs.plot(latencies[::n] + [latencies[-1]], ys[::n] + [ys[-1]], label = current_label, linewidth = 2, markersize = markersize, marker = marker, markevery = 0.1, color = line_color)

fig, axs = plt.subplots(nrows = 1, ncols = 1, figsize=(15,15))
axs.set_yscale('linear')
axs.set_xlabel("Latency (ms)", fontsize = x_label_font_size, color = 'black')
axs.set_ylabel("Cumulative Probability", fontsize = y_label_font_size, color = 'black')
#axs.set_xlim(left = -1, right = 250) #(xlim_percent * latencies[-1]) * 1.05)
axs.set_ylim(bottom = ylim_percent, top = 1.0125)
axs.tick_params(labelsize=xtick_font_size)
axs.xaxis.label.set_color('black')
axs.yaxis.label.set_color('black')

plot_start = time.time()
if input_hopsfs is not None:
    print("Plotting \"%s\" now..." % input_hopsfs)
    plot_data(input_hopsfs, axis = axs, label = label2, dataset = 2)
if input_lambdamds is not None:
    print("Plotting \"%s\" now..." % input_lambdamds)
    plot_data(input_lambdamds, axis = axs, label = label1, dataset = 1)
print("Plotted all data points in %f seconds" % (time.time() - plot_start))

if show_legend:
    fig.legend(loc = 'lower right', bbox_to_anchor=(0.95, 0.5))

plt.suptitle("Latency CDF - Spotify Workload - Log Scale x-Axis")
fig.tight_layout()

if output_path is not None:
  print("Saving plot to file '%s' now" % output_path)
  plt.savefig(output_path)
  print("Done")

if show_plot:
  print("Displaying figure now.")
  plt.show()