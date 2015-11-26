#!/usr/bin/env python

import csv
# from datetime import datetime
from dateutil import parser as dateparser
import argparse
import matplotlib.pyplot as plt
import numpy

def create_array_from_csv(csvfilename):
    log_status_array = []

    with open(csvfilename) as csv_log_file:
        csv_reader = csv.DictReader(csv_log_file)
        for logentry in csv_reader:
            time_diff = dateparser.parse(logentry['end_time']) - dateparser.parse(logentry['start_time'])
            if logentry['end_time']:
                time_diff_min = round(time_diff.seconds / 60.0, 1)
                log_status_array.append({'obsid': logentry['obsnum'], 'stage': logentry['stage'], 'start_time': logentry['start_time'], 'length_of_time': time_diff_min})
    return log_status_array

def main():
    avg_time_for_run = 0
    parser = argparse.ArgumentParser(description='AstroTaskr Workflow Management Software')
    parser.add_argument('-t', dest='graphtype', required=False,
                        help="type of graph")
    parser.add_argument('-f', dest='csvfilename', required=True,
                        help="Filename of .csv file")

    args, unknown = parser.parse_known_args()
    csvfilename = args.csvfilename    
    plt.rcParams['legend.loc'] = 'best'  # Put the legend in the best possible place
    plt.figure(1)
    
    log_status_array = create_array_from_csv(csvfilename)
    
    sorted_list_for_time = sorted(log_status_array, key=lambda k: k['start_time'])
    init_start_time = sorted_list_for_time[0]['start_time']
    global_end_time = ((dateparser.parse(sorted_list_for_time[len(sorted_list_for_time) - 1]['start_time']) - dateparser.parse(init_start_time)).seconds)/60
    global_end_time = global_end_time + (sorted_list_for_time[len(sorted_list_for_time) - 1]['length_of_time'])
    print("End time : %s") % (global_end_time)

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
                    time_since_beginning = dateparser.parse(log_entry_by_stage['start_time']) - dateparser.parse(init_start_time)
                    date_array_x.append((time_since_beginning.seconds)/60)
            plt.plot(date_array_x, time_array_y, '-o', label=logentry['stage'] + " - Avg: " + str(round(numpy.mean(time_array_y),1)) + " m" )
            avg_time_for_run = avg_time_for_run + round(numpy.mean(time_array_y),1)
            
    plt.title("No SGE - Wedge AWS 20 node run, 100 obsids")
    plt.ylabel("Time for task completion (min)")
    plt.xlabel('Time since beginning of run (min)')
    plt.grid(True)
    plt.xlim(0, global_end_time)
#    plt.ylim(-1, 60)
    #plt.yscale("log", nonposy='clip')
    plt.legend(framealpha=0.5)
    output_filename = csvfilename[:-4]
    print("Average total time : %s") % (avg_time_for_run)
    plt.savefig(output_filename, bbox_inches=None, pad_inches=0.1, dpi=150)

    plt.show()
    return 0

if __name__ == "__main__":
    main()
