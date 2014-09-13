#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# python cgi script that gathers form data from html form
# and passes it into the kmeans spark script
import sys, os, re, cgi, cgitb, calendar
from datetime import datetime

# enable debugging
cgitb.enable()

print "Content-Type: text/html"  #Header
print

# ------------- HELPERS ------------------#

# Validates a date. Dates should be in yyyy-mm-dd format
def validate_date(inputdate):
    if re.search(r'^\d{4}-\d{2}-\d{2}$', inputdate) is None:
        return False
    else:
        year = int(inputdate[0:4])
        month = int(inputdate[5:7])
        day = int(inputdate[8:10])
        if year > datetime.today().year:
            return False
        if month > 12 or month <=0:
            return False
        if day <=0 or day > calendar.monthrange(year,month)[1]:
            #monthrange[1] is number of days within a given month, in a given year
            return False
        return True


# ----------- Input validation ------------#

# Input response and validation
form = cgi.FieldStorage()
flag = True
validend = False

print "<h3>Server input:</h3>"
if "server" not in form:
    flag = False
    print "Could not find server input."
else:
    serverinput= form["server"].value
    print "The option chosen was: " + serverinput

print "<h3>Start date input:</h3>"
if "start_date" not in form:
    flag = False
    print "Could not find start date input."
else:
    startinput = form["start_date"].value
    if validate_date(startinput):
        print "Start date is: " + startinput
    else:
        flag = False
        print "Incorrect date format for start date"

print "<h3>End date input:</h3>"
if "end_date" not in form:
    print "Could not find end date input."
else:
    endinput = form["end_date"].value
    if validate_date(endinput):
        validend = True
        print "End date is: " + endinput
    else:
        print "Incorrect date format for end date"


# ---------------- Kmeans calling -----------#

if flag != True:
    print "<br /><br /> Due to an error in input, kmeans generation stopped. Please double check input before retrying."
else:
    print "<br /><br /> Accepting valid inputs. Generating new kmeans file."
    # pathing to called modules
    sys.path.insert(0, '/usr/hadoop/spark')
    if validend:
        sparkcmd =  "/usr/hadoop/spark/bin/spark-submit /usr/hadoop/spark/kmeans2_CSV.py \"%s\" \"%s\" \"%s\"" % (serverinput, startinput, endinput)
    else:
        sparkcmd =  "/usr/hadoop/spark/bin/spark-submit /usr/hadoop/spark/kmeans2_CSV.py \"%s\" \"%s\"" % (serverinput, startinput)
    os.popen(sparkcmd)

