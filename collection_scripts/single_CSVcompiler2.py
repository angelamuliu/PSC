#!/usr/bin/env python

# Takes in one argument - number of days ago you want to start copying from
# Runs multiple OpenTSDB HTTP API queries for PSC's Archiver server for data on
# CPU, disk IOBW, filewrites, filereads. Organizes it into a CSV file and places the
# compiled file onto the HDFS. Does this process for senses 0 - 7 (the servers
# that make up the Archiver) until today is reached.
#
# CSV lines are matched by time of filewrites collection rate
# CPU, diskIOBW, and net are averaged
##CSV format: time, filewrites, filereads, CPU, diskIOBW, net bytes
#
# ABOUT THE QUERIES
# filewrites/reads is not server specific, and is collected every 60s
# query responses are same size and tend to be around 700 data points
#
# CPU, disk IOBW, and net is server specific, and collected roughly every 10s
# query responses are close to same size and tend to be around 7000 data points

import sys, json, os
from datetime import datetime, timedelta

# To copy from from 12 days ago, days_ago = 12
# NOTE: You cannot gather data for today b/c filewrites/reads data takes time to come in
# To copy yesterday's data: days_ago = 2
def logformatter_call(days_ago):
    while days_ago > 1:
        logformatter(days_ago, days_ago-1)
        days_ago -= 1


# --------------------------------------------
# MAIN FUNCTION
# --------------------------------------------

# Given a start and end date (by days ago), formats a single day's log of CSV
# Repeats this process 8 times for sense0-7
def logformatter(start, end):
    try:
        # Collect json response by querying OpenTSDB, then load json
        ##json is wrapped in an array, get data EX: diskIOBW_json[0]['keyval']
        filewrites_out = querycollect("sum:file.writes.1m", "host=DSC", start, end)
        filereads_out = querycollect("sum:file.reads.1m", "host=DSC", start, end)
        
        filewrites_json = json.loads(filewrites_out)
        filereads_json = json.loads(filereads_out)
        
        filewrites_dpsdict = filewrites_json[0]['dps']
        filereads_dpsdict = filereads_json[0]['dps']
        # filewrites/reads is not server specific, so it doesn't go in the loop
        
        # For all data points, key is EPOCH time as a string integer
        # Make sorted list of all collection times (keys)
        filewrites_times = sorted(filewrites_json[0]['dps'].keys(), key=float)
        filereads_times = sorted(filereads_json[0]['dps'].keys(), key=float)
        
        for servernum in range(0,7): #range(0,8) for final
        # Repeating above process for diskIOBW, CPU, net bytes
            tags = "host=sense%s.psc.edu,op=*" % (str(servernum))
            diskIOBW_out = querycollect("sum:zpool.iobw", tags, start, end)
            
            tags = "host=sense%s.psc.edu" % (str(servernum))
            CPU_out = querycollect("sum:proc.loadavg.1m", tags, start, end)
            net_out = querycollect("sum:proc.net.bytes", tags, start, end)
            
            diskIOBW_json = json.loads(diskIOBW_out)
            CPU_json = json.loads(CPU_out)
            net_json = json.loads(net_out)
            
            diskIOBW_dpsdict = diskIOBW_json[0]['dps']
            CPU_dpsdict = CPU_json[0]['dps']
            net_dpsdict = net_json[0]['dps']
            
            diskIOBW_times = sorted(diskIOBW_dpsdict.keys(), key=float)
            CPU_times = sorted(CPU_json[0]['dps'].keys(), key=float)
            net_times = sorted(net_json[0]['dps'].keys(), key=float)
            
            # Creating logfile named yyyy-mm-dd-hh-MM to dump data into
            recordtime = datetime.now() - timedelta(start)
            filename = recordtime.strftime("%Y-%m-%d-%H-%M")
            logfile = open(filename, "w")
            
            keylengths = [len(filereads_times), len(filewrites_times), len(diskIOBW_times), len(CPU_times), len(net_times)]
            keylimit = min(keylengths) #Finding the smallest sized time list to iterate over
            
            keyiterate = 0
            diskIOBW_iterate = 0
            CPU_iterate = 0
            net_iterate = 0
            
            while keyiterate < keylimit:
                filewrites_key = filewrites_times[keyiterate]
                filereads_key = filereads_times[keyiterate]
                
                diskarr = findavg(filewrites_key, diskIOBW_iterate, diskIOBW_times, diskIOBW_dpsdict)
                CPUarr = findavgCPU(filewrites_key, CPU_iterate, CPU_times, CPU_dpsdict)
                netarr = findavg(filewrites_key, net_iterate, net_times, net_dpsdict)
                
                # time, filewrites, filereads, CPU, diskIOBW, net bytes
                writestring = "%s,%s,%s,%s,%s,%s\n" % (str(filewrites_key), str(filewrites_dpsdict[filewrites_key]),
                                                       str(filereads_dpsdict[filereads_key]), str(CPUarr[0]), str(diskarr[0]), str(netarr[0]))
                logfile.write(writestring)
                
                diskIOBW_iterate = diskarr[1]
                CPU_iterate = CPUarr[1]
                net_iterate = netarr[1]
                keyiterate = keyiterate + 1
            logfile.close()
            
            #Place file into hadoop and delete local log
            statinfo = os.stat(filename)
            if statinfo.st_size == 0:
                print "WARNING: Filesize is 0. Something went wrong.. Not placing file into HDFS."
            else:
                print "Attempting to place IO logfile %s for sense%s into HDFS . . ." % (filename, str(servernum))
                hadoopcommand = "hadoop fs -put %s hdfs://10.0.0.4:8020/opentsdb/sense%s" % (filename, str(servernum))
                os.popen(hadoopcommand)
            removecommand = "rm %s" % (filename)
            os.popen(removecommand)
    except:
        print "usage: python large_CSVcompiler"
        raise


# --------------------------------------------
# HELPERS
# --------------------------------------------

# Given a metric and time frame, returns the raw json response from OpenTSDB
##For queries that don't require a server number
def querycollect(metric, tags, start, end):
    # Creating strings of queries to be run
    ##wget: get data from online, quiet: don't spit to terminal, -O - :  reroute output to next thing it is assigned to
    if end == 0:
        querystr = "wget --quiet -O - \"http://clemente.psc.edu:4243/api/query?start=%sd-ago&m=%s{%s}\"" % (str(start), metric, tags)
    else:
        querystr = "wget --quiet -O - \"http://clemente.psc.edu:4243/api/query?start=%sd-ago&end=%sd-ago&m=%s{%s}\"" % (str(start), str(end), metric, tags)
    return os.popen(querystr).read()

# Find out which query started recording the latest, that will be starting point
# in regards to time
def findstart(writes, reads):
    if int(writes[0]) > int(reads[0]):
        start = int(writes[0])
    else:
        start = int(reads[0])
    return start

# Returns an average value based on rough 60s range around a given time
# Also returns back the iterator value so we don't have to iterate from the
# begining every time
def findavg(time, iterator, keylist, dpsdict):
    dps = []
    while int(keylist[iterator]) < (int(time) + 30):
        key = keylist[iterator]
        dps.append(dpsdict[key])
        iterator = iterator + 1
    if len(dps) < 1:
        avg = dpsdict[keylist[iterator]]
    else:
        avg = sum(dps) / float(len(dps))
    return [avg, iterator]

# CPU already is finding averages so just select a value once a time is passed
def findavgCPU(time, iterator, keylist, dpsdict):
    while int(keylist[iterator]) < int(time):
        iterator = iterator + 1
    avg = dpsdict[keylist[iterator]]
    return [avg, iterator]

    
# --------------------------------------------

# Number is days ago to drag data from
# 2 -> start = 2 days ago, end = 1 day ago
logformatter_call(2)