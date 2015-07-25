#!/usr/bin/python

import os,sys

dateToBeSearched = sys.argv[1]

if dateToBeSearched == "":
   print "Please Provide date for which you want to list available backups"
   sys.exit(1)

listBackupDirsForDate = "grep " + dateToBeSearched + " metadata | awk '{print $1}'"

backupDirsStringForDate = os.popen(listBackupDirsForDate).read()

print backupDirsStringForDate
