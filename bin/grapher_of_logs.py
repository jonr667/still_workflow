#!/usr/bin/env python

import csv
# from datetime import datetime
from dateutil import parser
import matplotlib.pyplot as plt


def make_plot(date_array_x, time_array_y, stage_name, global_end_time):
    plt.rcParams['legend.loc'] = 'best'  # Put the legend in the best possible place
    plt.figure(1)
    plt.plot(date_array_x, time_array_y, label=stage_name, marker='o')
    plt.title(stage_name)
    plt.ylabel("Time to completion (min)")
    plt.xlabel('Time since beginning of run (sec)')
    filename = "stage_" + stage_name
    plt.xlim(0, global_end_time)
    plt.savefig(filename, bbox_inches=None, pad_inches=0.1, dpi=150)
    plt.show()


def main():
    log_status_array = []
    with open('log_entries.csv') as csv_log_file:
        csv_reader = csv.DictReader(csv_log_file)
        for logentry in csv_reader:
            time_diff = parser.parse(logentry['end_time']) - parser.parse(logentry['start_time'])
            if logentry['end_time']:
                time_diff_min = round(time_diff.seconds / 60.0, 1)
                log_status_array.append({'stage': logentry['stage'], 'start_time': logentry['start_time'], 'length_of_time': time_diff_min})

    sorted_list_for_time = sorted(log_status_array, key=lambda k: k['start_time'])
    init_start_time = sorted_list_for_time[0]['start_time']
    global_end_time = (parser.parse(sorted_list_for_time[len(sorted_list_for_time) - 1]['start_time']) - parser.parse(init_start_time)).seconds
    global_end_time = global_end_time + (sorted_list_for_time[len(sorted_list_for_time) - 1]['length_of_time'] * 60)

    sorted_list = sorted(log_status_array, key=lambda k: (k['stage'], k['start_time']))

    list_of_processed_stages = []
    for logentry in sorted_list:
        if logentry['stage'] not in list_of_processed_stages:
            list_of_processed_stages.append(logentry['stage'])
            time_array_y = []
            date_array_x = []
            for log_entry_by_stage in sorted_list:
                if log_entry_by_stage['stage'] == logentry['stage']:
                    time_array_y.append(log_entry_by_stage['length_of_time'])
                    time_since_beginning = parser.parse(log_entry_by_stage['start_time']) - parser.parse(init_start_time)
                    date_array_x.append(time_since_beginning.seconds)
            make_plot(date_array_x, time_array_y, logentry['stage'], global_end_time)

    return 0

if __name__ == "__main__":
    main()
