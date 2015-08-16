#!/usr/bin/python
import sys, os
import datetime
import re
import yaml
from boto.s3.connection import S3Connection
import config
import inspect
import socket
from os import path
from findRestoreSnapshot import getDateTimeObjectToString
from findRestoreSnapshot import getListOfRestorePoint
from findRestoreSnapshot import getDateObjectFromString

# Get CASSANDRA_HOME
def getCassandraHome():
        cassandra_home=os.environ['CASSANDRA_HOME']
        #cassandra_home=config.cassandraHome
        if cassandra_home != "":
                 return cassandra_home
        else :
                print "Please set the environment variable CASSANDRA_HOME"
                sys.exit(1)

def getCassandraDataDir(cassandra_home):
        cassandraConfigFile =  cassandra_home+ "/conf/cassandra.yaml"
        with open(cassandraConfigFile, 'r') as configFile:
                yamlKeyValuePairs = yaml.load(configFile)
        dataFileDirectory = yamlKeyValuePairs["data_file_directories"]
        return dataFileDirectory[0]

def restoreKeySpace():
        cassandra_home = getCassandraHome();
        cassandraDataDir = getCassandraDataDir(cassandra_home)

        print "Stopping Cassandra..."

        # Warning Message for Stopping Cassandra, Once User confirm with Y script will continue futher operation
        length=len(sys.argv)
        if ( length <=3 ):
                userInput = raw_input("Do you want to continue [Y/n]?")
                if userInput == "Y":
                        print("Cassandra Stopped")
                else:
                        sys.exit(1)
        os.system("pgrep -f cassandra | xargs kill")

        commitLogsLocation = "rm -rf " + cassandraDataDir+ "/../commitlog/*"
        print "Deleting commit logs from " + commitLogsLocation

        # Warning Message for commitlog, Once User confirm with Y script will continue futher operation
        if ( length <=3 ):
                userInput = raw_input("Do you want to continue [Y/n]?")
                if userInput == "Y":
                        print("Deleted commitlog")
                else:
                        sys.exit(1)
        os.system(commitLogsLocation)

        keySpaceSSTLocation=cassandraDataDir+"/"+keyspaceName+"/"
        print "Deleting Key Space SST table from " + keySpaceSSTLocation
        deletSSTTablesCommand = "rm -rf " + keySpaceSSTLocation

        # Warning Message for Removing Keyspace directory, Once User confirm with Y script will continue futher operation
        if ( length <=3 ):
                userInput = raw_input("Do you want to continue [Y/n]?")
                if userInput == "Y":
                        print("Deleted Key Space SST table")
                else:
                        sys.exit(1)
        os.system ( deletSSTTablesCommand)

        currentContextFolderPath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory
		
	# Download Snapshot 
        s3SnapshotDirectory = "s3://" + bucketName + "/" + nodeName  + "/snapshots/" + keyspaceName + "/" + lastSnapshot + "/"
        s3RemoteDataSyncCommand = "aws s3 sync " + s3SnapshotDirectory + " " + keySpaceSSTLocation
	print "Downloading backed up (Snapshots + Incremental) content in the local Cassandra folder from:"+ s3SnapshotDirectory + " to " + keySpaceSSTLocation
        os.system(s3RemoteDataSyncCommand)
	
	# Download Incremental Backup
	
	s3IncrementalDir = "s3://" + bucketName + "/" + nodeName  + "/incrementalBackup/" + keyspaceName + "/" + lastSnapshot + "/"
	
	s3IncrementalDirList = "aws s3 ls " + s3IncrementalDir
	print s3IncrementalDirList
	columnFamiliesList = os.popen(s3IncrementalDirList).readlines()
	print columnFamiliesList
        for CF in columnFamiliesList:
               	s3ColumnFamilyPath = s3IncrementalDir + CF.rstrip('/').split()[1]
		print s3ColumnFamilyPath
		s3ColumnFamilyPathCommand =  "aws s3 ls " + s3ColumnFamilyPath
		filesInColumnFamily = os.popen(s3ColumnFamilyPathCommand).readlines()
		print filesInColumnFamily
		for files in filesInColumnFamily:
			fileName=files.split(' ')
			length = len(fileName)
			print fileName[length-1].rstrip()
			downloadIncrementalBackup = "aws s3 cp " + s3ColumnFamilyPath + fileName[length-1].rstrip() + " " + keySpaceSSTLocation + "/" + CF.rstrip('/').split()[1]
			os.system(downloadIncrementalBackup)

	
        print "Starting Cassandra..."
        cassadraStartShellCommand=cassandra_home + "/bin/cassandra"
        os.system(cassadraStartShellCommand);


# Get Keyspace
keyspaceName = sys.argv[1]
restoreFolderName = sys.argv[2]
if keyspaceName == "":
   print "Please Provide Keyspace to Restore"
   sys.exit(1)

bucketName = "cassandra-backup-dir"
nodeName = (socket.gethostname())
nodeS3Path = "s3://" + bucketName + "/" + nodeName
# List last snapshot
def listLastSnapshot():
        listOfRestorePointInDateTimeObject = getDateObjectFromString(getListOfRestorePoint(nodeS3Path,keyspaceName))
        getSnapshots = getDateTimeObjectToString(listOfRestorePointInDateTimeObject,restoreFolderName)
        return getSnapshots
lastSnapshot = listLastSnapshot()

restoreKeySpace()

