import sys, os, re
from pyspark import SparkContext, SparkConf
from operator import add

#--------------------------------------------------------#
# MAIN FUNCTIONS

# Given a dir path, finds useful aggregate info (mean, number of logs, etc...)
# about the server and each of that server's shelf
def find_shelf_aggregates():
    sconf = SparkConf().setAppName("Find shelf info")
    sc = SparkContext(conf=sconf)
    if len(sys.argv) != 2:
	print "You didn't give the program any folder to look into!"
    else:
	foldfiles = get_file_paths(sys.argv[1])
	#NOTE: wholetextfiles is bugged with hdfs reading. Find way of getting
	#file paths from hdfs in another fashion...
	results_RDD = compile_RDD(sc, foldfiles)
	
	server_name = find_server_name(sc, foldfiles[0])
	server_mean = results_RDD.values().mean()
	
	#shelves_sum = [ ("shelfname", sum_of_temps)... ]
	shelves_sums = results_RDD.reduceByKey(add).sortByKey().collect()
	#shelves_numlogs = { ("shelfname", num_of_logs) ... }
	shelves_numlogs = results_RDD.countByKey()
	
	print "------------------------------"
	print "FOR SERVER - %s" % (server_name)
	print "Mean Tempterature (C): %f" % (server_mean)
	print "------------------------------"
	shelf_results = []
	for shelf in shelves_sums:
	    shelf_numlogs = shelves_numlogs[shelf[0]]
	    shelf_mean = float(shelf[1]) / shelf_numlogs
	    print "For shelf %s, the mean temperature is %f" % (shelf[0], shelf_mean)
	print "------------------------------"


#--------------------------------------------------------#
# HELPER FUNCTIONS

# Reads a single shelf file and returns array of (k,v) where k->shelf, v->temp
def gettemp(sc, filepath):
    fileData = sc.textFile(filepath)
    shelfdat = fileData.filter(lambda s: 'Shelf' in s).collect()
    result = []
    for line in shelfdat:
	#Using regex to select only info on shelf and temp
    	shelf = re.search( r'sdisk\w\d-(rear|front)', line)
    	temp = re.search( r'\d\d C', line)
    	if shelf and temp:
	    #If found, we convert regex into strings and appends to result as kv
    	    convert_temp = int(re.search(r'\d\d', temp.group()).group())
    	    convert_shelf = str(shelf.group())
    	    result.append( (convert_shelf, convert_temp))
    return result

# Returns array of full file paths of directory given (foldpath)
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

# Given a collection of paths, compiles a single RDD of (k,v) pairs that are
# shelf and temp values
def compile_RDD(sc, foldfiles):
    results_RDD = sc.parallelize([])
    for currfile in foldfiles:
	#iterate through all files and create single currfile_RDD, a spark dict
	#with (k,v) pairs associated with the file
	currfile_RDD = sc.parallelize(gettemp(sc, currfile))
	#Merge said spark dict to a results dict that holds A LOT of (k,v)
	results_RDD = results_RDD.union(currfile_RDD)
    return results_RDD

# Reads a single shelf file and returns string of overall server name
def find_server_name(sc, filepath):
    fileData = sc.textFile(filepath)
    serverline = fileData.filter(lambda s: 'Shelf' in s).first()
    server = re.search( r'sense\d', serverline)
    return str(server.group())
    

#--------------------------------------------------------#
# RUN FUNCTIONS

# to run on cluster: /bin/spark-submit hdfs_shelftemp.py <args>
# EX: /bin/spark-submit hdfs_shelftemp.py "hdfs://10.0.0.4:8020/sense2/sense_shelftemp/sense0"
find_shelf_aggregates()

