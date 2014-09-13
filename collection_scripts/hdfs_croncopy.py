#!/usr/bin/env python

#HDFS Deepcopy v5 "croncopy" - Usage:
#This python script recursively copies contents of a directory (argument 1)
#into a directory on an hdfs cluster (argument 2)

#This version is time sensitive to the hour and is to be used w/ cron
#If this script is run at 11:00 am, it will be sensitive to files collected at 11:00 am

#Note:
#The program will replace ':' with '-' in copied file names

import os, sys, time

def deepCopy_call(localpath, hdfspath):
    hadoopmake = "hadoop fs -mkdir \"%s\"" % (hdfspath)
    os.popen(hadoopmake)
    deepCopy(localpath, hdfspath)

def deepCopy(localpath, hdfspath):
    try:
        for d in os.listdir(localpath):
            if os.path.isdir(localpath + "/" + d):
                print "DIR: Copying dir %s and files . . ." % (d)
                hdfs_copy = "hadoop fs -mkdir " + "\"%s/%s\"" % (hdfspath, d)
                os.popen(hdfs_copy)
                deepCopy(localpath + "/" + d, hdfspath + "/" + d)
            else:
                file_timelastedit = os.stat(localpath + "/" + d).st_mtime
                if timecompare(file_timelastedit):
                    if d.count(":") > 0: #Clean filename of colons
                        print "FILE: Colons in name found. Cleaning and copying."
                        cleaned_filename = nocolons(d)
                        symlink = "ln -s %s /usr/hadoop/templink" % (d)
                        os.popen(symlink) #symlinking to prevent hdfs put errors regarding :
                        copypath = "hadoop fs -put /usr/hadoop/templink %s/%s" % (hdfspath, cleaned_filename)
                        os.popen(copypath)
                        os.popen("unlink /usr/hadoop/templink")
                    else:
                        print "FILE: Copying file %s" % (d)
                        copypath = "hadoop fs -put %s %s" % (d, hdfspath)
                        os.popen(copypath)
                else:
                    print ">> File too old to copy"
    except:
        "Usage: deepcopy(localpath, hdfspath)"
        raise


# HELPERS
#--------------------------------

#Returns true if file is newer than an hour
def timecompare(filetime):
    #filetime = epoch time of when file was last edit
    #i.e. 1403293081
    # is the file older than one hour (7200 seconds)
    if int(filetime) < (int(time.time()) - 7200):
        print "Older than one hour"
        return False
    else:
        print "Newer than one hour"
        return True
    
# Given a filename removes colons
def nocolons(filename): 
    try:
        if filename.find("/") >= 0:
            filename=filename.split("/")[-1]
        filename=filename.strip().replace(":","-")
        return filename
    except:
        print "usage: ./nocolons.py /path/to/filename"
        raise

deepCopy_call("/usr/hadoop/sensedatamount/sense_disktemp", "hdfs://10.0.0.4:8020/sense2/sense_disktemp")
deepCopy_call("/usr/hadoop/sensedatamont/sense_shelftemp", "hdfs://10.0.0.4:8020/sense2/sense_shelftemp")