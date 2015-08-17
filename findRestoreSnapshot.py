#!/usr/bin/python
import datetime, time, sys
from datetime import datetime, timedelta
import os
def getListOfRestorePoint(nodeS3Path,keySpace):
        listOfRestorePoint = "aws s3 ls " + nodeS3Path + "/snapshots/" + keySpace + "/"
        #listOfRestorePoint = "aws s3 ls s3://cassandra-backup-dir/sandip/snapshots/demo/"
        RestorePoints = os.popen(listOfRestorePoint).readlines()
        newlist = []
        for RP in RestorePoints:
                newlist.append(RP.rstrip('/').split()[1].strip('/'))
        return newlist

def getDateObjectFromString(listOfDates):
        listOfRestorePoint = []
        counter = 0
        for dateString in listOfDates:
                date = datetime(year=int(dateString[0:4]), month=int(dateString[4:6]), day=int(dateString[6:8]))
                listOfRestorePoint.append(date)
        return listOfRestorePoint

def getDateTimeObjectToString(listOfRestorePointInDateTimeObject,baseRestorePoint):
        date1 = datetime(year=int(baseRestorePoint[0:4]), month=int(baseRestorePoint[4:6]), day=int(baseRestorePoint[6:8]))

        closestTime = min(listOfRestorePointInDateTimeObject,key=lambda date : abs(date1-date))
        restorePoint = closestTime.strftime('%Y%m%d')
	return restorePoint
baseRestorePoint = sys.argv[1]
#listOfRestorePointInDateTimeObject = getDateObjectFromString(getListOfRestorePoint(nodeS3Path,keySpace))
#getDateTimeObjectToString(listOfRestorePointInDateTimeObject,baseRestorePoint)
