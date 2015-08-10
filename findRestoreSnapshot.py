#!/usr/bin/python
import datetime, time, sys
from datetime import datetime, timedelta
import os
def getListOfRestorePoint():
        listOfRestorePoint = "aws s3 ls " + nodeS3Path + "/snapshots/" + keySpace + "/"
        #listOfRestorePoint = "aws s3 ls s3://cassandra-backup-dir/sandip/snapshots/demo/"
        RestorePoints = os.popen(listOfRestorePoint).readlines()
        newlist = []
        for RP in RestorePoints:
                newlist.append(RP.rstrip('/').split()[1].strip('/'))

        return newlist
#print getListOfRestorePoint()

def getDateObjectFromString(listOfDates):
        listOfRestorePoint = []
        counter = 0
        for dateString in listOfDates:
                #date = datetime(year=int(dateString[0:4]), month=int(dateString[4:6]), day=int(dateString[6:8]), hour=int(dateString[8:10]), minute=int(dateString[10:12]), second=int(dateString[12:14]))
		date = datetime(year=int(dateString[0:4]), month=int(dateString[4:6]), day=int(dateString[6:8]))
                listOfRestorePoint.append(date)
        return listOfRestorePoint

def getDateTimeObjectToString(listOfRestorePointInDateTimeObject):
        #date1 = datetime(year=int(baseRestorePoint[0:4]), month=int(baseRestorePoint[4:6]), day=int(baseRestorePoint[6:8]), hour=int(baseRestorePoint[8:10]), minute=int(baseRestorePoint[10:12]), second=int(baseRestorePoint[12:14]))
		
	date1 = datetime(year=int(baseRestorePoint[0:4]), month=int(baseRestorePoint[4:6]), day=int(baseRestorePoint[6:8]))
		
        closestTime = min(listOfRestorePointInDateTimeObject,key=lambda date : abs(date1-date))
        #restorePoint = closestTime.strftime('%Y%m%d%H%M%S')
	restorePoint = closestTime.strftime('%Y%m%d')
        #print restorePoint

baseRestorePoint = sys.argv[1]
listOfRestorePointInDateTimeObject = getDateObjectFromString(getListOfRestorePoint())
getDateTimeObjectToString(listOfRestorePointInDateTimeObject)

