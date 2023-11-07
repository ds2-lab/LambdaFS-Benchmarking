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

from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset

plt.style.use('ggplot')
mpl.rcParams['text.color'] = 'black'
mpl.rcParams['xtick.color'] = 'black'
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.rcParams['ytick.color'] = 'black'
mpl.rcParams["figure.figsize"] = (8,6)

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

parser.add_argument("-ylim", default = 0.0, type = float, help = "Set the limit of each y-axis to this percent of the max value.")
parser.add_argument("-xlim", default = 1.0, type = float, help = "Set the limit of each x-axis to this percent of the max value.")

parser.add_argument("-n", default = 1, type = int, help = "Plot every `n` points (instead of all points).")

parser.add_argument("--show", action = 'store_true', help = "Show the plot rather than just write it to a file")

parser.add_argument("-o", "--output", type = str, default = None, help = "File path to write chart to. If none specified, then don't write to file.")
parser.add_argument("-c", "--columns", default = ["timestamp", "latency", "worker_id", "path"], nargs='+')

args = parser.parse_args()

input_hopsfs = args.input_hopsfs
input_lambdamds = args.input_lambdamds
xlim_percent = args.xlim
ylim_percent = args.ylim
n = args.n 
show_plot = args.show
output_path = args.output
COLUMNS = args.columns

print(COLUMNS)

vanilla_colors = ["#ffa822", "#124c6d", "#ff6150", "#1ac0c6", "#7c849c", "#6918b4", "#117e16", "#ff7c00", "#ff00c5"]
lambda_colors = ["#cc7a00", "#0b2e42", "#b31200", "#128387", "#424757", "#410f70", "#0c5a10", "#b35600", "#cc009c"]

def plot_data(input_path, axis = None, vanilla = False):
    # If we pass a single .txt file, then just create DataFrame from the .txt file.
    # Otherwise, merge all .txt files in the specified directory.
    if input_path.endswith(".txt"):
        df = pd.read_csv(input_path)
    else:
        print("input_path: " + input_path)
        print("joined: " + str(os.path.join(input_path, "*.txt")))
        all_files = glob.glob(os.path.join(input_path, "*.txt"))

        if vanilla:
            framework_name = "Writes" #"HopsFS"
            colors = vanilla_colors
            marker = "X"
            markersize = 8
        else:
            framework_name = "No Writes" #r'$\lambda$' + "MDS"
            colors = lambda_colors
            marker = "*"
            markersize = 10

        row = 0
        col = -1

        # Merge the .txt files into a single DataFrame.
        for i, filename in enumerate(all_files):
            print("Reading file: " + filename)
            df = pd.read_csv(filename, index_col=None, header=0)
            df.columns = COLUMNS

            # Sort the DataFrame by timestamp.
            df = df.sort_values('latency')

            latencies = df['latency'].values.tolist()

            fs_operation_name = os.path.basename(filename)[:-4] # remove the ".txt" with `[:-4]`
            current_label = "%s %s" % (framework_name, fs_operation_name)

            ys = list(range(0, len(latencies)))
            ys = [y / len(ys) for y in ys]

            axis[row, col].plot(latencies[::n] + [latencies[-1]], ys[::n] + [ys[-1]], label = current_label, linewidth = 2, markersize = markersize, marker = marker, markevery = 0.1, color = colors[i])
            axis[row, col].set_yscale('linear')
            axis[row, col].set_xlabel("Latency (ms)", fontsize = x_label_font_size)
            axis[row, col].set_ylabel("Cumulative Probability", fontsize = y_label_font_size)
            axis[row, col].tick_params(labelsize=xtick_font_size)
            axis[row, col].set_title(fs_operation_name)
            #axis[row, col].set_xlim(left = -0.1, right = (xlim_percent * latencies[-1]) * 1.05)
            axis[row, col].set_ylim(bottom = ylim_percent, top = 1.0125)

            row = (row + 1) % 3

            if (row == 0):
                col = (col + 1) % 3

fig, axs = plt.subplots(nrows = 3, ncols = 3, figsize=(15,15))

plot_start = time.time()
if input_hopsfs is not None:
    plot_data(input_hopsfs, axis = axs, vanilla = True)
if input_lambdamds is not None:
    plot_data(input_lambdamds, axis = axs, vanilla = False)
print("Plotted all data points in %f seconds" % (time.time() - plot_start))

#fig.legend()
plt.suptitle("Latency CDF - Spotify Workload - Log Scale x-Axis")
fig.tight_layout()
# axs.set_yscale('linear')
# axs.set_xlabel("Latency (ms)", fontsize = x_label_font_size)
# axs.set_ylabel("Cumulative Probability", fontsize = y_label_font_size)
# axs.tick_params(labelsize=xtick_font_size)
# axs.set_title("CDF - Spotify Workload - Log Scale x-Axis")

if output_path is not None:
  print("Saving plot to file '%s' now" % output_path)
  plt.savefig(output_path)
  print("Done")

if show_plot:
  print("Displaying figure now.")
  plt.show()