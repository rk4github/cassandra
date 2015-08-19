#!/usr/bin/python

import inspect
from subprocess import call
import subprocess
import datetime
from glob import glob
import getopt, sys, os
import boto
from boto.exception import S3ResponseError
import yaml
import ntpath
import socket
from files_syncer import getNewlyAddedFiles
from files_syncer import getListOfDeletedFiles
from files_syncer import getMasterFilesList
from findRestoreSnapshot import getDateTimeObjectToString
from findRestoreSnapshot import getListOfRestorePoint
from findRestoreSnapshot import getDateObjectFromString

keyspace = sys.argv[1]
def validateKeyspace():
	if keyspace == "":
		print "Please Provide Keyspace to Backup"
		sys.exit(1)

def getCassandraHome():
    cassandraHome=''
    try:
        cassandraHome = os.environ['CASSANDRA_HOME']
    except KeyError:
        print "Please set the environment variable CASSANDRA_HOME"
        sys.exit(1)
    return cassandraHome
cassandraHome = getCassandraHome()
	
def cassandraDataDir():
    cassandraConfigFile =  cassandraHome+"/conf/cassandra.yaml"
    with open(cassandraConfigFile, 'r') as f:
        keys = yaml.load(f)
    dataFileDirectory = keys["data_file_directories"]
    return dataFileDirectory[0]

def createIncrementalBackup():
	NODETOOL = cassandraHome +'/bin/nodetool'
	print 'Creating Incremental Backup For ' + keyspace + ' ..........'
	call([NODETOOL, "flush", keyspace])

def getIncrementalBackupDirectoriesPath():
	incrementalBackUpList = cassandraDataDir() + "/" + keyspace + "/*/backups/"
	keyspacePath = cassandraDataDir() + "/" + keyspace
	incrementalBackUpColumnFamilyPaths = glob(incrementalBackUpList)
	return incrementalBackUpColumnFamilyPaths
	
def getNodeBackupS3Path():
	bucketName = "cassandra-backup-dir"
	nodeName = (socket.gethostname())
	nodeS3Path = "s3://"+bucketName+"/"+nodeName
	return nodeS3Path

def getLatestAvailableSnapshotFolderNameAtS3():
        nodeS3Path = getNodeBackupS3Path()
	s3SnapshotFolderName = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
	listOfRestorePointInDateTimeObject = getDateObjectFromString(getListOfRestorePoint(nodeS3Path,keyspace))
        return getDateTimeObjectToString(listOfRestorePointInDateTimeObject,s3SnapshotFolderName)

def syncIncrementalBackupIntoLastSnapshotFolder():
        validateKeyspace()
        createIncrementalBackup()
        nodeS3Path = getNodeBackupS3Path()
        latestAvailableSnapshotFolder = getLatestAvailableSnapshotFolderNameAtS3()
        syncDir = 'snapshots'
	action = 'incremental' # It's important for deciding sync or snapshots directory for uploading files
        for incrementalColumnFamilyPath in getIncrementalBackupDirectoriesPath():
                keyspacePath=incrementalColumnFamilyPath.split("/backups")[0]
                keyspaceAndColumnFamilies=keyspacePath.split(cassandraDataDir())[1]
                columnFamily=keyspaceAndColumnFamilies.split("/")[2]
                incrementalBackupSyncDir = nodeS3Path + "/" + syncDir + "/" + keyspace + "/" + latestAvailableSnapshotFolder + "/" + columnFamily

                for files in getNewlyAddedFiles(incrementalColumnFamilyPath,nodeS3Path,syncDir,keyspace,latestAvailableSnapshotFolder,columnFamily,action):
                        os.chdir(incrementalColumnFamilyPath)
                        s3SyncCommand = "aws s3 cp " + files + " " + incrementalBackupSyncDir + "/" + files
                        print "Syncing Differential Incremental Backup: <Local-2-S3>"
                        print (files)
                        print "Executing: " + s3SyncCommand + " to sync incremental backup of keyspace " + keyspace + " for cloumn family " + columnFamily
                        os.system(s3SyncCommand)

syncIncrementalBackupIntoLastSnapshotFolder()

	
