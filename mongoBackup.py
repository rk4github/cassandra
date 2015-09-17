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
        outputFileName = collectionName + ".json"
        newListOfOutputFileNames.append(outputFileName)
        commandToExportMongoCollection = "mongoexport --db "+databaseName+ " --collection "+collectionName+ " --out "+outputFileName
        print "Exporting Mongodb Collection " + collectionName + " from Database "+databaseName
        os.system(commandToExportMongoCollection)
    return newListOfOutputFileNames

def getNodeBackupS3Path():
    nodeName = (socket.gethostname())
    nodeS3Path = "s3://"+bucketName+"/"+nodeName
    return nodeS3Path

def uploadMongoCollectionsToS3():
    outputFileNames = exportMongoCollections()
    s3SyncDir = getNodeBackupS3Path()
    for collectionName in outputFileNames:
        backupTime = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')
        s3SyncCommand = "aws s3 cp "+ collectionName + " " + s3SyncDir + "/mongoBackup/" + backupTime+"/" +dbName+"/"+ collectionName
        print "Uploading Mongodb Collection : <Local-2-S3>"
        print s3SyncCommand
        os.system(s3SyncCommand)

def validateDatabase():
    client = MongoClient()
    databases = client.database_names()
    if any(dbName in s for s in databases):
        uploadMongoCollectionsToS3()
    else:
        print "Please Provide a Valid Database Name"

def main():
    validateDatabase()

dbName = sys.argv[1]
bucketName = sys.argv[2]

main()


