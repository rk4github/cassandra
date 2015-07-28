#!/usr/bin/python
import datetime, time, sys
from datetime import datetime, timedelta

def getListOfRestorePoint():
        with open('metadata') as f:
                listOfRestorePoint = [line.split()[0] for line in f]
        return listOfRestorePoint

def getDateObjectFromString(listOfDates):
        listOfRestorePoint = []
        counter = 0
        for dateString in listOfDates:
                date = datetime(year=int(dateString[0:4]), month=int(dateString[4:6]), day=int(dateString[6:8]), hour=int(dateString[8:10]), minute=int(dateString[10:12]), second=int(dateString[12:14]))
                listOfRestorePoint.append(date)
        return listOfRestorePoint

def getDateTimeObjectToString(listOfRestorePointInDateTimeObject):
        date1 = datetime(year=int(baseRestorePoint[0:4]), month=int(baseRestorePoint[4:6]), day=int(baseRestorePoint[6:8]), hour=int(baseRestorePoint[8:10]), minute=int(baseRestorePoint[10:12]), second=int(baseRestorePoint[12:14]))
        closestTime = min(listOfRestorePointInDateTimeObject,key=lambda date : abs(date1-date))
        restorePoint = closestTime.strftime('%Y%m%d%H%M%S')
        print restorePoint

baseRestorePoint = sys.argv[1]
listOfRestorePointInDateTimeObject = getDateObjectFromString(getListOfRestorePoint())
getDateTimeObjectToString(listOfRestorePointInDateTimeObject)
