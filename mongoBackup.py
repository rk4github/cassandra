import sys
import datetime
import os
from pymongo import MongoClient
import socket

def getCollectionNames():
    client = MongoClient()
    collectionNames = client[dbName].collection_names()
    return collectionNames

def exportMongoCollections():
    databaseName = dbName
    newListOfOutputFileNames = []
    for collectionName in getCollectionNames():
        backupTime = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')
        outputFileName = collectionName + backupTime + ".json"
        newListOfOutputFileNames.append(outputFileName)
        commandToExportMongoCollection = "mongoexport --db "+databaseName+ " --collection "+collectionName+ " --out "+outputFileName
        print "Exporting Mongodb Collection " + collectionName + " from Database "+databaseName
        os.system(commandToExportMongoCollection)
    return newListOfOutputFileNames

def getNodeBackupS3Path():
    bucketName = sys.argv[2]
    nodeName = (socket.gethostname())
    nodeS3Path = "s3://"+bucketName+"/"+nodeName
    return nodeS3Path

def uploadMongoCollectionsToS3():
    outputFileName = exportMongoCollections()
    s3SyncDir = getNodeBackupS3Path()
    for collectionName in outputFileName:
        s3SyncCommand = "aws s3 cp "+ collectionName + " " + s3SyncDir + "/mongodb/" +dbName+"/"+ collectionName
        print "Uploading Mongodb Collections : <Local-2-S3>"
        print s3SyncCommand
        os.system(s3SyncCommand)


def main():
    uploadMongoCollectionsToS3()

dbName = sys.argv[1]
main()


