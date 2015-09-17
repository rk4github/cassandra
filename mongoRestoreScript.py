import sys
import socket
import os
from os import listdir
from os.path import isfile, join
from pymongo import MongoClient


def getNodeBackupS3Path():
    nodeName = (socket.gethostname())
    nodeS3Path = "s3://"+bucketName+"/"+nodeName
    return nodeS3Path


def downloadMongoCollectionsFromS3():
    databaseName = dbName
    restorePointOfMongo = restorePoint
    nodeS3Path = getNodeBackupS3Path()
    commandToDownloadS3Directory = "aws s3 sync " + nodeS3Path + "/mongoBackup/" + restorePointOfMongo+"/" + databaseName+" " + databaseName
    print "Downloading Mongodb Collection : <S3-2-Local>"
    os.system(commandToDownloadS3Directory)

def getCollectionList():
    restorePointDirectoryContainingCollections = dbName
    collectionsInRestorePointDirectory = [ collection for collection in listdir(restorePointDirectoryContainingCollections) if isfile(join(restorePointDirectoryContainingCollections,collection)) ]
#    collectionsInRestorePointDirectory = os.popen(restorePointDirectoryContainingCollections).readlines()
    return collectionsInRestorePointDirectory

def importCollectionsIntoMongoDB():
    databaseName = dbName
    for collection in getCollectionList():
        collectionNameWithOutJson = collection.split('.')[0]
        commandToImportCollectionIntoMongo = "mongoimport --db "+databaseName+ " --collection "+collectionNameWithOutJson+ " < "+databaseName+"/"+collection
        os.system(commandToImportCollectionIntoMongo)

def cleanDatabaseBeforeImportCollections():
    mongodatabaseInstanceConnection = MongoClient()
    databaseName = dbName
    for collection in getCollectionList():
        collectionName = collection.split('.')[0]
        mongodatabaseInstanceConnection[databaseName].drop_collection(collectionName)   

def validateDatabase():
    client = MongoClient()
    databases = client.database_names()
    if any(dbName in s for s in databases):
        print "*************"
    else:
        print "Please Provide a Valid Database Name"
    
def main():
    if optionToCleanUPCollectionBeforeRestore == 'restoreDatabaseWithCleanUP':
        validateDatabase()
        cleanDatabaseBeforeImportCollections()
        downloadMongoCollectionsFromS3()
        importCollectionsIntoMongoDB()
    else:
        if optionToCleanUPCollectionBeforeRestore == 'restoreDatabaseWithOutCleanUP':
            validateDatabase()
            downloadMongoCollectionsFromS3()
            importCollectionsIntoMongoDB()
        else:
            print "Please Select Database Clean-up before Restore Operation"
        
dbName = sys.argv[1]
restorePoint = sys.argv[2]
bucketName = sys.argv[3] 
optionToCleanUPCollectionBeforeRestore = sys.argv[4]

main()
