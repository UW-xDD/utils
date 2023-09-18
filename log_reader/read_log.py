#!/usr/bin/env python
# encoding: utf-8

import glob
from datetime import datetime
import sys

VERBOSE=False


def parse_time(line):
    """
    Parse the time of submission/termination/execution from a line
    Assumes: the log is from the current year
    Args:
        line (string): Line from the process.log file.
            expected input is of the type:
                "001 (132663.000.000) 12/29 15:16:50 Job executing on host: <128.105.244.247:35219>"
    Returns:
        Datetime object
    """
    now = datetime.now()
    line = line.split(" ")
    timeString = line[2] + " " + line[3]
    date_object = datetime.strptime(timeString, '%Y-%m-%d %H:%M:%S')
    if date_object.month == 12 and now.month == 1: # we're looking at a log from Dec, but we're in Jan
        date_object = date_object.replace(year=now.year - 1) # so it actually belongs in last year
    else:
        date_object = date_object.replace(year=now.year) # ASSUMES the log is from the current year.
    return date_object

def parse_resources(chunk):
    """
    Takes a chunk of log text that includes the "Partitionable Resources" table
    and returns a parsed dict of {"diskUsage": xxx, "memUsage": yyy }
    Args:
        chunk (string): The text block containing the Partitional Resources table from standard HTCondor output.
    Returns:
        Dictionary of the form usage = { memUsage: (memory usage in MB), diskUsage: (disk usage in KB) }
    """
    usage = {}
    # break the chunk up by \n
    chunk = chunk.split("\n")
    for line in chunk:
        if "Disk" in line:
            numbers = [int(s) for s in line.split() if s.isdigit()]
            usage["diskUsage"] = numbers[0]
        if "Memory" in line:
            numbers = [int(s) for s in line.split() if s.isdigit()]
            usage["memUsage"] = numbers[0]
    return usage

def read_log(path):
    """
    Reads log to determine disk/mem usage, runtime
    For processing time, it will only grab the last execution/evict/terminated times.
    And runTime supercedes evictTime (eg. an exec->evict combination will not be written if
    a later exec-termination combination exists in the log)
    To be appended to processing database, so that structure is:
    ocr_processing["tag"]["jobs"] = [ {startTime: xxx, execTime: yyy, ... }, {reports from other jobs...} ]
    Args:
        jobpath (string): path to the job output
    Returns:
        If successful, returns dict of the form::
            jobReport = { subTime: (time of submission),
                            execTime: (start time of latest execution),
                            evictTime: (time of job eviction, if any),
                            termTime: (time of job termination, if any),
                            runTime: (time between execution start and termination/eviction time),
                            usage: { usage dictionary from above},
                        }
        Otherwise, it returns None
    """
    try:
        with open(path) as logfile:
            chunk = ""
            jobReport = {}
            jobReport["path"] = path
            for line in logfile:
                if line.startswith("..."):
                    if chunk.startswith("000"): # submitted
                        jobReport["subTime"] = parse_time(chunk.split('\n')[0])
                    elif chunk.startswith("001"): # executing
                        jobReport["execTime"] = parse_time(chunk.split('\n')[0])
                    elif chunk.startswith("004"): # evicted, has partitionable table
                        jobReport["evictTime"] = parse_time(chunk.split('\n')[0])
                        runTime = (jobReport["evictTime"] - jobReport["execTime"])
                        jobReport["runTime"] = runTime.days * 86400 + runTime.seconds
                        jobReport["usage"] = parse_resources(chunk)
                    elif chunk.startswith("005"): # termination, has partitionable table
                        jobReport["termTime"] = parse_time(chunk.split('\n')[0])
                        runTime = (jobReport["termTime"] - jobReport["execTime"])
                        jobReport["runTime"] = runTime.days * 86400 + runTime.seconds
                        jobReport["usage"] = parse_resources(chunk)
                    elif chunk.startswith("006"):
                        pass
                    elif chunk.startswith("009"):
                        jobReport["abortedTime"] = parse_time(chunk.split('\n')[0])
#                        runTime = (jobReport["abortedTime"] - jobReport["execTime"])
#                        jobReport["runTime"] = runTime.days * 86400 + runTime.seconds
                    else:
                        if VERBOSE:
                            print("UNKNOWN CODE")
                            print(chunk)
                    chunk = ""
                else:
                    chunk += line
        return jobReport
    except IOError:
        print("Couldn't find file at %s" % path)
        return None

def main():
    """
    TODO: Docstring for main.
    Returns: TODO

    """
    if len(sys.argv) <= 1 or sys.argv[1] not in ["time", "memory", "disk"]:
        print("Invalid selection! Please specify mode (time/memory)")
        sys.exit(1)
    if len(sys.argv) > 2:
        pattern = sys.argv[2]
    else:
        pattern = "job*/process.log"
    pattern = pattern.strip()
    for f in glob.glob(pattern):
#        print("%s" % f)
        try:
            dummy = read_log(f)
            if sys.argv[1] == "time":
                print(dummy["runTime"])
            elif sys.argv[1] == "memory":
                print(dummy["usage"]["memUsage"])
            elif sys.argv[1] == "disk":
                print(dummy["usage"]["diskUsage"])
        except:
#            print("Couldn't parse field!")
#            print(sys.exc_info())
            continue


if __name__ == '__main__':
    main()
