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


def getCassandraHome():
	cassandraHome=''
	try:
		cassandraHome = os.environ['CASSANDRA_HOME']
        except KeyError:
		print "Please set the environment variable CASSANDRA_HOME"
		sys.exit(1)
	return cassandraHome

def  createSnapshot(keyspace):
	# Get Keyspace
	if keyspace == "":
			print "Please Provide Keyspace to Backup"
			sys.exit(1)

	cassandraHome=getCassandraHome()

        NODETOOL = cassandraHome +'/bin/nodetool'
        # Snapshot format
        s3SnapshotFolderName = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
        snapshotDate = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')

        # Clear Previous Snapshots for the provided keyspace
        call([NODETOOL, "clearsnapshot", keyspace])

        # Create snapshots for provided keyspace
        print 'Creating Snapshots For ' + keyspace + ' at ' + snapshotDate + ' ..........'
        call([NODETOOL, "snapshot", "-t", snapshotDate, keyspace])

        nodeS3Path = getNodeBackupS3Path()
        syncSnapshotContentToS3SyncDirAndCreateSnapshot(nodeS3Path,keyspace,snapshotDate,s3SnapshotFolderName)


def syncSnapshotContentToS3SyncDirAndCreateSnapshot(nodeS3Path,keyspace,snapshotDate,s3SnapshotFolderName):
        # Get Current working directory
        scriptExecutionPath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory
        snapshotDirColumnFamilyPaths = getLocalColumnFamilyPathsToSync(keyspace,snapshotDate)
        syncDir = 'sync_dir'
        action = 'backup' # It's important for deciding sync or snapshots directory for uploading files
	s3SnapshotFolderName = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
        for snapshotDirColumnFamilyPath in snapshotDirColumnFamilyPaths:
                columnFamily=getColumnFamilyName(snapshotDirColumnFamilyPath)
		s3SyncDir = nodeS3Path + "/sync_dir/"+keyspace + "/"+columnFamily
        	os.chdir(snapshotDirColumnFamilyPath)
		for newFile in getNewlyAddedFiles(snapshotDirColumnFamilyPath,nodeS3Path,syncDir,keyspace,s3SnapshotFolderName,columnFamily,action):
			copyFileToS3(newFile,s3SyncDir)

		for toBeRemovedFile in getListOfDeletedFiles(snapshotDirColumnFamilyPath,nodeS3Path,syncDir,keyspace,columnFamily,action,s3SnapshotFolderName):
			deleteFilesFromS3(toBeRemovedFile,s3SyncDir)

        os.chdir(scriptExecutionPath)
        createSnapshotAtS3(nodeS3Path, keyspace, snapshotDate,columnFamily,s3SyncDir)

def getLocalColumnFamilyPathsToSync(keyspace,snapshotDate):
        expressionForSnapshotsDirList = getCassandraDataDir()+"/"+keyspace+"/*/snapshots/"+snapshotDate
        keyspacePath = getCassandraDataDir()+"/"+keyspace
        snapshotDirColumnFamilyPaths = glob(expressionForSnapshotsDirList)
        return snapshotDirColumnFamilyPaths

def getColumnFamilyName(snapshotDirColumnFamilyPath):
	keyspacePath=snapshotDirColumnFamilyPath.split("/snapshots")[0]
	key=keyspacePath.split(getCassandraDataDir())[1]
	columnFamily=key.split("/")[2]
	return columnFamily


def getNodeBackupS3Path():
	bucketName = "cassandra-backup-dir"
	nodeName = (socket.gethostname())
	nodeS3Path = "s3://"+bucketName+"/"+nodeName
	return nodeS3Path

def copyFileToS3(newFile,s3SyncDir):
	s3SyncCommand = "aws s3 cp "+ newFile + " " + s3SyncDir + "/" + newFile
	print "Syncing Differential Snapshot: <Local-2-S3>"
	os.system(s3SyncCommand)

def deleteFilesFromS3(toBeRemovedFile,s3SyncDir):
	s3RemoveCommand = "aws s3 rm " + s3SyncDir + "/" + toBeRemovedFile
	print "Removing deleted Files: <From-S3>"
	os.system(s3RemoveCommand)

def createSnapshotAtS3(nodeS3Path, keyspace, snapshotDate,columnFamily,s3SyncDir):
	s3SnapshotDirectory = nodeS3Path + "/snapshots/"+keyspace+"/"+snapshotDate+"/"+columnFamily
	s3RemoteCopyCommand = "aws s3 sync " + s3SyncDir + " " + s3SnapshotDirectory
	print "Creating Snapshot: <S3-3-S3>"
	os.system(s3RemoteCopyCommand)


def getCassandraDataDir():
	cassandraHome=getCassandraHome()
        cassandraConfigFile =  cassandraHome+"/conf/cassandra.yaml"

        with open(cassandraConfigFile, 'r') as f:
                keys = yaml.load(f)

        dataFileDirectory = keys["data_file_directories"]
        return dataFileDirectory[0]

createSnapshot(sys.argv[1])
