#!/usr/bin/python

import sys, os, re
from operator import add
from datetime import datetime, timedelta

from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt

# EX hdfs path: "hdfs://10.0.0.4:8020/opentsdb/sense0"
# EX running: /bin/spark-submit kmeans2_CSV.py "sense0" "2014-07-20" "2014-07-24"

#--------------------------------------------------------#
# Main Program
#--------------------------------------------------------#

# Given a path to files, does kmeans clustering ML analysis
def kmeans_CSV():
    try:
	# creating a parsedData RDD to do kmeans on
	servernum = sys.argv[1]
	serverpath = "hdfs://10.0.0.4:8020/opentsdb/" + servernum
	print "Attempting to create SparkContext"
	sconf = SparkConf().setAppName("Kmeans for files")
	print "Sconf set..."
	sc = SparkContext(conf=sconf)
	print "SparkContext created"
	
	# making parsedData RDD: [ array([filewrites, filereads, CPU, diskIOBW, net bytes]), array([...]), ... ]
	# kmeans iteratively passes over data multiple times - cache parsedData
	
	if len(sys.argv) == 2: #user just specified server - do full server kmeans
	    filepaths = get_file_paths(serverpath)# Array of string file paths to all files within folder
	    parsedData = compile_RDD(sc, filepaths).cache()
	    CSV_filename =  make_name(filepaths) + "_" + servernum
	elif len(sys.argv) == 3: #user put in server and single timeframe - do single file kmeans
	    timeframe = sys.argv[2] #ex: 2014-07-09
	    filepaths = get_singlefile_path(timeframe, serverpath)
	    parsedData = compile_RDD(sc, filepaths).cache()
	    CSV_filename = str(timeframe) + "_" + servernum
	else: #user put in server and start/end timeframe - do timeframe kmeans
	    start_timeframe = sys.argv[2]
	    end_timeframe = sys.argv[3]
	    filepaths = get_timeframefile_paths(start_timeframe, end_timeframe, serverpath)
	    parsedData = compile_RDD(sc, filepaths).cache()
	    CSV_filename =  make_name(filepaths)+ "_" + servernum
	k = findk(parsedData.count())
	
	clusters = KMeans.train(parsedData, k, maxIterations=10, runs=10, initializationMode="random")
	centers = clusters.clusterCenters
	
	# Creating two CSVs (one has data points, one has centers) for later visualization
	compile_CSV(CSV_filename, parsedData)
	compile_centers_CSV(CSV_filename, centers)
	print "SUCCESS: Kmeans done"
    except:
	print "---------------------------------"
	print "Usage: ./bin/spark-submit kmeans_CSV.py <servername> <start_timeframe> <end_timeframe>"
	print "<servername> must be specified. EX: sense0 "
	print "Timeframes are optional. Specify just one timeframe for single file kmeans. Specify start and end for kmeans over timeframe."
	print "Timeframes must be in format yyyy-mm-DD"
	print "---------------------------------"
	raise



#--------------------------------------------------------#
# These are helper functions for the main programs

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point, clusters):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

# Returns array of full file paths of directory given (foldpath)
# Note that if foldpath contains a folder, it will not 'go in' 
def get_file_paths(foldpath):
    result = []
    o = os.popen("hadoop fs -ls %s" % foldpath)
    l = o.readlines()
    for line in l:
        filepath = re.search(r'/(.+)', line)
        if filepath is not None:
            fullfilepath = "hdfs://10.0.0.4:8020" + str(filepath.group())
            result.append(fullfilepath)
    return result

# Returns an array of a single file's path
def get_singlefile_path(start_timeframe, serverpath):
    result = []
    o = os.popen("hadoop fs -ls %s" % serverpath)
    l = o.readlines()
    for line in l:
	filepath = re.search(r'\/%s.{6}' % start_timeframe, line)
	if filepath is not None:
	    fullfilepath = serverpath + str(filepath.group())
	    result.append(fullfilepath)
    return result

# Returns an array of full file paths matching within a timeframe
def get_timeframefile_paths(start_timeframe, end_timeframe, serverpath):
    st = datetime.strptime(start_timeframe, "%Y-%m-%d") #convert time strings to dates
    et = datetime.strptime(end_timeframe, "%Y-%m-%d")
    result = []
    o = os.popen("hadoop fs -ls %s" % serverpath)
    l = o.readlines()
    for line in l:
	if st > et:
	    return result
	filepath = re.search(r'\/%s.{6}' % st.strftime("%Y-%m-%d"), line)
	if filepath is not None:
	    fullfilepath = serverpath + str(filepath.group())
	    result.append(fullfilepath)
	    st = st + timedelta(1)
    return result

# Given an array containing file paths, compiles a single RDD from all files
def compile_RDD(sc, fold_filepaths):
    compiled_RDD = sc.parallelize([])
    for currfile in fold_filepaths:
	data = sc.textFile(currfile)
	# We don't want time in the kmeans analysis. [1:] cuts the time value off the CSV
	parsedData = data.map(lambda line: array([float(x) for x in line.split(',')[1:]]))
	compiled_RDD = compiled_RDD.union(parsedData)
    return compiled_RDD

# Finds a very....general k value
def findk(num_of_points):
    klimit = sqrt(float(num_of_points))
    return int(klimit/2)

# Makes a CSV filename string given an array of filepaths
# filepaths named yyyy-mm-dd-HH-MM of time collected
def make_name(filepaths):
    # Extract yyyy-mm-dd of first file used
    start = re.search(r'([^\/]+$)', filepaths[0]).group()[0:10]
    # Extract mm-dd of last file used
    end = re.search(r'([^\/]+$)', filepaths[-1]).group()[5:10]
    namestring = start + "_" + end
    return namestring

# Creates a final CSV of all used values for visualization
# kmeans CSV files are saved in /var/www/d3/csvs
def compile_CSV(CSV_filename, parsedData):
    filename = "/var/www/d3/csvs/" + CSV_filename + "_kmeansCSV.csv"
    logfile = open(filename, "w")
    logfile.write("filewrites,filereads,CPU,diskIOBW,net bytes\n") #header
    i = 0
    datalist = parsedData.collect()
    while i < len(datalist):
	line = datalist[i]
	writestring = "%s,%s,%s,%s,%s\n" % (str(line[0]), str(line[1]), str(line[2]), str(line[3]), str(line[4]))
	logfile.write(writestring)
	i += 1
    logfile.close()
    
# Creates a CSV of centers
# kmeans CSV files are saved in /var/www/d3/csvs
def compile_centers_CSV(CSV_filename, centersarray):
    filename = "/var/www/d3/csvs/" + CSV_filename + "_kmeansCentersCSV.csv"
    logfile = open(filename, "w")
    logfile.write("filewrites,filereads,CPU,diskIOBW,net bytes\n") #header
    i = 0
    while i < len(centersarray):
	line = centersarray[i]
	writestring = "%s,%s,%s,%s,%s\n" % (str(line[0]), str(line[1]), str(line[2]), str(line[3]), str(line[4]))
	logfile.write(writestring)
	i += 1
    logfile.close()

#--------------------------------------------------------#

kmeans_CSV()