#!/usr/bin/python

import inspect
from subprocess import call
import subprocess
import datetime
from glob import glob
import getopt, sys, os
import boto
from boto.exception import S3ResponseError
import config
import yaml
import ntpath
import socket
from files_syncer import getNewlyAddedFiles
from files_syncer import getListOfDeletedFiles
from files_syncer import getMasterFilesList
from findRestoreSnapshot import getDateTimeObjectToString
from findRestoreSnapshot import getListOfRestorePoint
from findRestoreSnapshot import getDateObjectFromString

# Get Keyspace
KEYSPACE = sys.argv[1]

if KEYSPACE == "":
   print "Please Provide Keyspace to Backup"
   sys.exit(1)

# Get CASSANDRA_HOME
try:
        cassandraHome = os.environ['CASSANDRA_HOME']
except KeyError:
   print "Please set the environment variable CASSANDRA_HOME"
   sys.exit(1)

NODETOOL = cassandraHome +'/bin/nodetool'   

# Get cassandraDataDir
def getCassandraDataDir():
        cassandraConfigFile =  cassandraHome+"/conf/cassandra.yaml"

        with open(cassandraConfigFile, 'r') as f:
                keys = yaml.load(f)

        dataFileDirectory = keys["data_file_directories"]
        return dataFileDirectory[0]



# Snapshot format
s3SnapshotFolderName = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
snapshot = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')
# Clear Previous Snapshots for the provided keyspace
call([NODETOOL, "clearsnapshot", KEYSPACE])

# Create snapshots for provided keyspace
print 'Creating Snapshots For ' + KEYSPACE + ' at ' + snapshot + '..........'
call([NODETOOL, "snapshot", "-t", snapshot, KEYSPACE])

# Get Snapshots Lists
expressionForSnapshotsDirList = getCassandraDataDir()+"/"+KEYSPACE+"/*/snapshots/"+snapshot
keyspacePath = getCassandraDataDir()+"/"+KEYSPACE
snapshotDirColumnFamilyPaths = glob(expressionForSnapshotsDirList)

# Get Current working directory
PATH = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory

bucketName = "cassandra-backup-dir"
nodeName = (socket.gethostname())
nodeS3Path = "s3://"+bucketName+"/"+nodeName

# List last snapshot
def listLastSnapshot():
        listOfRestorePointInDateTimeObject = getDateObjectFromString(getListOfRestorePoint(nodeS3Path,KEYSPACE))
        getSnapshots = getDateTimeObjectToString(listOfRestorePointInDateTimeObject,s3SnapshotFolderName)
        return getSnapshots
lastSnapshot = listLastSnapshot()

for snapshotDirColumnFamilyPath in snapshotDirColumnFamilyPaths:
        keyspacePath=snapshotDirColumnFamilyPath.split("/snapshots")[0]
        b=keyspacePath.split(getCassandraDataDir())[1]
        columnFamily=b.split("/")[2]

        s3SyncDir = nodeS3Path + "/sync_dir/"+KEYSPACE + "/"+columnFamily
	syncDir = 'sync_dir'
        for files in getNewlyAddedFiles(snapshotDirColumnFamilyPath,syncDir,KEYSPACE,nodeS3Path,columnFamily):
                os.chdir(snapshotDirColumnFamilyPath)
                s3SyncCommand = "aws s3 cp "+ files + " " + s3SyncDir + "/" + files
                print "Syncing Differential Snapshot: <Local-2-S3>"
                print (files)
                print "Executing: " + s3SyncCommand + " to sync snapshot of keyspace " + KEYSPACE + " for cloumn family " + columnFamily
		os.system(s3SyncCommand)
		
        for files in getListOfDeletedFiles(snapshotDirColumnFamilyPath,syncDir,KEYSPACE,nodeS3Path,columnFamily):
                s3RemoveCommand = "aws s3 rm " + s3SyncDir + "/" + files
                print "Removing deleted Files: <From-S3>"
                print (files)
                print "Executing: " + s3RemoveCommand + " to Deleted of Files From " + KEYSPACE + " for cloumn family " + columnFamily
                os.system(s3RemoveCommand)
        os.chdir(PATH)

        s3SnapshotDirectory = nodeS3Path + "/snapshots/"+KEYSPACE+"/"+s3Snapshot+"/"+columnFamily
        s3RemoteCopyCommand = "aws s3 sync " + s3SyncDir + " " + s3SnapshotDirectory
        print "Creating Snapshot: <S3-3-S3>"
        print "Executing s3 remote copy command : " + s3RemoteCopyCommand
        
        os.system(s3RemoteCopyCommand)


	# List last snapshot
	incrementalBackupSyncDir = nodeS3Path + "/incrementalBackupSyncDir/" + KEYSPACE + "/" + columnFamily
        #incrementalBackup = nodeS3Path + "/incrementalBackup/" + KEYSPACE + "/" + s3Snapshot + "/" + columnFamily 
	incrementalBackup = nodeS3Path + "/incrementalBackup/" + KEYSPACE + "/" + lastSnapshot + "/" + columnFamily
	incrementalBackupDirCommand = "aws s3 sync " + incrementalBackupSyncDir + " " + incrementalBackup
	cleanIncrementalBackupSyncDir = "aws s3 ls " + nodeS3Path + "/incrementalBackupSyncDir/" + KEYSPACE + "/" + columnFamily + "/"
	print "Creating Incremental Backup: <S3-2-S3>"
	print "Executing: " + incrementalBackupDirCommand + " to clean incremental sync directory for " + KEYSPACE + "/" + columnFamily 
	os.system(incrementalBackupDirCommand)

	filesInS3SyncDir = os.popen(cleanIncrementalBackupSyncDir).readlines()

	for files in filesInS3SyncDir:
                fileName=files.split(' ')
                length = len(fileName)
		removeFiles = "aws s3 rm " + incrementalBackupSyncDir + "/" + fileName[length-1].rstrip()
		print fileName[length-1].rstrip()
		os.system(removeFiles)


