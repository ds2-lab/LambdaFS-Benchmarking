import argparse
import numpy as np
import pandas as pd
import time
import random
import matplotlib as mpl
import matplotlib.pyplot as plt
import glob
import os
import yaml 
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset, inset_axes
import matplotlib.ticker as ticker
from ast import literal_eval as make_tuple

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
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.rcParams["figure.figsize"] = (16,8)

font = {'weight' : 'bold',
        'size'   : 32}
mpl.rc('font', **font)

x_label_font_size = 42
y_label_font_size = 42
xtick_font_size = 40
#markersize = 10
#linewidth = 4

parser = argparse.ArgumentParser()

parser.add_argument("-i", "--input", type = str, default = None, help = "Path to input file. Each line contains a pair of the form \"<path>;<label>\", specifying an input and its associated label. This is an alternative to passing specific inputs via the -i1 -l1 -i2 -l2 -i3 -l3 flags.")

# parser.add_argument("-i1", "--input1", dest="input1", help = "\lambdaMDS Spotify 25k.")
# parser.add_argument("-i2", "--input2", dest="input2", help = "\lambdaMDS Spotify 50k.")
# parser.add_argument("-i3", "--input3", dest="input3", help = "HopsFS Spotify 25k.")
# parser.add_argument("-i4", "--input4", dest="input4", help = "HopsFS Spotify 50k.")

parser.add_argument("-l1", "--label1", default = r'$\lambda$', help = "Label for first set of data.") # r'$\lambda$' + "MDS"
parser.add_argument("-l2", "--label2", default =  "H", help = "Label for second set of data.")

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

input_file_path = args.input 

# lambdamds_25k_input_path = args.input1
# lambdamds_50k_input_path = args.input2
# hopsfs_25k_input_path = args.input3
# hopsfs_50k_input_path = args.input4

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

input1_colors = ["#FF5656", "#279CDF", "#FF9550", "#1AC632", "#7c849c", "#6918b4", "#EB2D8C", "#00F6FF", "#757781"]
input2_colors = ["#BF4141", "#1D75A7", "#BF703C", "#149526", "#5D6375", "#4F1287", "#B02269", "#00B9BF", "#585961"]
input3_colors = ["#802B2B", "#144E6F", "#804B28", "#0D6319", "#3E424E", "#350C5A", "#751646", "#007B80", "#3A3B41"]
input4_colors = ["#401515", "#0A2738", "#402514", "#07320D", "#1F2127", "#1A062D", "#3B0B23", "#003D40", "#1D1E20"]

base_colors = ["tab:red", "tab:blue", "tab:green", "tab:orange", "#8755b5", "#d738a7", "tab:brown", "tab:cyan", "tab:olive", "tab:gray", "tab:brown"]
color_adjustment = [0.25, 0.5, 0.75, 1.0, 1.25, 1.5]

MARKERS = ['x', '.', 'X', 'o', 'v', '^', '<', '>']
ops = {}
sub_axis = {}
next_idx = 0

name_mapping = {
    "DELETE": "DELETE",
    "GETLISTING": "LS DIR",
    "GETFILEINFO": "STAT",
    "MKDIRS": "MKDIR",
    "GETBLOCKLOCATIONS": "READ",
    "RENAME": "MV"
}

averages_1 = {}
averages_2 = {}

counts_1 = {}
counts_2 = {}

df_s_create = None
df_s_complete = None

ONLY_PLOT_THESE = [] # ["READ"]

def adjust_lightness(color, amount=0.5):
    import matplotlib.colors as mc
    import colorsys
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], max(0, min(1, amount * c[1])), c[2])

def plot_data(input_yaml, dataset = 0, axis = None):
    global ops
    global next_idx
    global df_s_complete
    global df_s_create

    input_path = input_yaml["path"]
    label = input_yaml.get("label", "No-Label-Specified")
    
    print("Label: %s" % label)

    # Adding the 'or' part ensures that, if an empty value is specified in the yaml (i.e., "markersize: " with no number), then we still default to a valid value.
    marker = input_yaml.get("marker", ".") or "."
    markersize = input_yaml.get("markersize", 8) or 8
    linestyle = input_yaml.get("linestyle", "solid") or "solid"
    linewidth = input_yaml.get("linewidth", 4) or 4
    markevery = input_yaml.get("markevery", 0.1)
    columns = input_yaml.get("columns", ["timestamp", "latency"])

    # The user may have specified something like (10, (10, 10)), which is a format matplotlib interprets to add offset and whatnot to the dashes. 
    if type(linestyle) is str:
        try:
            linestyle = make_tuple(linestyle)
            print("Setting linestyle to tuple: %s (type is %s)" % (str(linestyle), type(linestyle)))
        except:
            pass 

    if len(marker) > 1:
        try:
            marker = make_tuple(marker)
            print("Setting marker to tuple: %s (type is %s)" % (str(marker), type(marker)))
        except:
            pass 

    num_cold_starts = 0

    # If we pass a single .txt file, then just create DataFrame from the .txt file.
    # Otherwise, merge all .txt files in the specified directory.
    # if input_path.endswith(".txt"):
    #     df = pd.read_csv(input_path)
    # else:
    all_files = glob.glob(os.path.join(input_path, "*.txt"))
    
    colors = []
    for base_color in base_colors:
        color = adjust_lightness(base_color, amount = color_adjustment[dataset])
        colors.append(color)

    idx = 0

    # Merge the .txt files into a single DataFrame.
    for filename in all_files:
        fs_operation_name = os.path.basename(filename)[:-4].upper() # remove the ".txt" with `[:-4]`

        if len(ONLY_PLOT_THESE) > 0 and fs_operation_name not in ONLY_PLOT_THESE:
            continue

        print("Reading file: " + filename)
        num_starts_for_op = 0
        df = pd.read_csv(filename, index_col=None, header=0, )
        df.columns = columns

        # Sort the DataFrame by timestamp.
        df = df.sort_values('latency')
        # df['latency'] = df['latency'].map(lambda x: x / 1.0e6)

        latencies = df['latency'].values.tolist()

        #if (dataset == 1 or dataset == 2):
        if (fs_operation_name in name_mapping):
            fs_operation_name = name_mapping[fs_operation_name]
        elif fs_operation_name == "COMPLETE":
            df_s_complete = df

            if (df_s_create is not None):
                df_s_create['latency'] = df_s_create['latency'] + df_s_complete['latency']
                df = df_s_create
                fs_operation_name = "CREATE FILE"
            else:
                continue
        elif fs_operation_name == "CREATE":
            df_s_create = df

            if (df_s_complete is not None):
                df_s_create['latency'] = df_s_create['latency'] + df_s_complete['latency']
                df = df_s_create
                fs_operation_name = "CREATE FILE"
            else:
                continue

        current_label = "%s - %s" % (label, fs_operation_name)
        
        print("Current label: %s" % current_label)

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

        print("max(latencies): ", max(latencies))

        ys = list(range(0, len(latencies)))
        ys = [y / len(ys) for y in ys]
        
        axis[idx].plot(latencies[::n] + [latencies[-1]], ys[::n] + [ys[-1]], label = label, linewidth = linewidth, linestyle = linestyle, markersize = markersize, marker = marker, markevery = markevery, color = colors[idx])
        #axis[idx].plot(latencies, ys, label = current_label, linewidth = 2, markersize = markersize, marker = marker, markevery = 0.1, color = colors[idx])
        axis[idx].set_yscale('linear')
        axis[idx].set_xlabel("Latency (ms)", fontsize = x_label_font_size)
        if idx == 0:
            axis[idx].set_ylabel("Cumulative Probability", fontsize = y_label_font_size)
        axis[idx].tick_params(labelsize=xtick_font_size)
        axis[idx].set_title(fs_operation_name, fontdict={"fontsize": 40})
        #axis[idx].set_xlim(left = -1, right = 250) #(xlim_percent * latencies[-1]) * 1.05)
        axis[idx].set_ylim(bottom = ylim_percent, top = 1.0125)
        axis[idx].xaxis.label.set_color('black')
        axis[idx].yaxis.label.set_color('black')
        axis[idx].yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.2f}"))

        #if fs_operation_name != "MKDIR":
        if fs_operation_name in sub_axis:
            axins = sub_axis[fs_operation_name]
        else:
            axins = inset_axes(axis[idx], 6, 3, bbox_transform=axis[idx].transAxes, bbox_to_anchor=(0.95, 0.915))
            axins.set_xlim(left = -10, right = min(latencies[-1] * 0.25, 200))
            axins.set_ylim(bottom = 0.95, top = 1.00125)
            axins.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.2f}"))
            axins.yaxis.set_major_locator(ticker.MultipleLocator(0.02))
            axins.tick_params(labelsize = xtick_font_size + 4)
            sub_axis[fs_operation_name] = axins

        axins.plot(latencies[::n] + [latencies[-1]], ys[::n] + [ys[-1]], label = label, linewidth = linewidth, linestyle = linestyle, markersize = markersize * 0.875, marker = marker, markevery = markevery, color = colors[idx])

    print("Removed a total of %d points." % num_cold_starts)

num_columns = 7 #max(num_input1_files, num_input2_files)
if len(ONLY_PLOT_THESE) > 0:
    num_columns = len(ONLY_PLOT_THESE)

print("Plotting data now...")

fig, axs = plt.subplots(nrows = 1, ncols = num_columns, figsize=(81, 10))
plot_start = time.time()

with open(input_file_path, 'r') as input_file:
    inputs = yaml.safe_load(input_file) 

print("There are %d input(s) to plot." % len(inputs))

for i, input in enumerate(inputs):
    print("\n\n\nPlotting dataset %d/%d: '%s'. Path: '%s'" % (i+1, len(inputs), input["label"], input["path"]))
    plot_data(input, dataset = i, axis = axs)
    
    df_s_complete = None 
    df_s_create = None 

print("Done. Plotted all data points in %f seconds." % (time.time() - plot_start))

if skip_plot:
    exit(0)

if show_legend:
    for ax in axs:
        leg = ax.legend(loc = 'lower left', prop={'size': 44}, labelspacing=0.16, framealpha=0.0, handlelength=0.9, handletextpad = 0.175, ncol=2, columnspacing = 0.2, bbox_to_anchor = (0.0725, -0.0145), borderaxespad = 0.05)
        if leg:
            leg.set_zorder(999)
            leg.set_draggable(state = True)
        ax.xaxis.set_major_formatter(ticker.EngFormatter(sep=""))
        ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.2f}"))

fig.tight_layout()
plt.tight_layout()
plt.subplots_adjust(wspace=0.16)

if show_plot:
  print("Displaying figure now.")
  plt.show()

if output_path is not None:
  print("Saving plot to file '%s' now" % output_path)
  plt.savefig(output_path)
  print("Done")