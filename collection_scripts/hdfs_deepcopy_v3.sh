#!/bin/bash

#HDFS Deepcopy v3 - Usage:
#This bash script recursively copies contents of a directory (argument 1)
#into a directory on an hdfs cluster (argument 2) given a timeframe to start
#copying from (argument 3)

#This is not meant to be used as a cron job

#EX usage: . hdfs_deepCopy_v3.sh "local/path/of/a/dir" "hdfs://IP:PORT/path/to/dir" "YYYYMMDD"

#Note:
#The program will replace ':' with '-' in copied file names

function deepCopy_call {
	hadoop fs -mkdir "${2}"
	deepCopy "${1}" "${2}" "${3}"
}

function deepCopy {
	for d in ${1}/*;
	do
		if [ -d "${d}" ]; then #is a dir
			echo "DIR: Copying dir ${d} and files . . ."
			hadoop fs -mkdir "${2}/$(basename ${d})"
			count="$( find ${d} -mindepth 1 -maxdepth 1 | wc -l )"
	  		if [ $count -eq 0 ]; then #empty folder, don't bother recalling
	  			echo "Found empty folder"
	  		else
				deepCopy "${d}" "${2}/$(basename ${d})" "${3}"
			fi
	  	else #is a file
	  		dashformat=${3:0:4}-${3:4:2}
	  		day=${3:6:2}
	  		fileday=$(echo ${d: -8:2} | sed 's/^0*//') #strip away leading zero
	  		if [[ ${d} == *${3:0:6}* || ${d} == *${dashformat}* ]]; then #file must have date in name
	  			if (( ${day} <= ${fileday} )); then
			  		if [[ ${d} == *:* ]]; then #colon cleaning
						nocolon=`python /usr/hadoop/nocolons.py $(basename ${d})`
						echo "FILE: Colon in filename $(basename ${d}) found. Renamed to ${nocolon}. Copying."
						ln -s ${d} /usr/hadoop/templink #symlinking to prevent put errors regarding :
						hadoop fs -put /usr/hadoop/templink ${2}/${nocolon}
						unlink /usr/hadoop/templink
					else
						echo "FILE: Copying file ${d}"
						hadoop fs -put ${d} ${2}
					fi
				fi
			fi
	  	fi
	done;
}

#EX: deepCopy_call "/usr/hadoop/sense2/sense_shelftemp/sense5" "hdfs://10.0.0.4:8020/sense2/sense_shelftemp/sense5" "20140605"

deepCopy_call "/usr/hadoop/sensedatamount/sense_disktemp" "hdfs://10.0.0.4:8020/sense2/sense_disktemp" "20140606"
deepCopy_call "/usr/hadoop/sensedatamont/sense_shelftemp" "hdfs://10.0.0.4:8020/sense2/sense_shelftemp" "20140606"

#deepCopy_call ${1} ${2} ${3}