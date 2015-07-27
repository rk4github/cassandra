#!/usr/bin/python

#import temp
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

# Get Keyspace
KEYSPACE = sys.argv[1]

if KEYSPACE == "":
   print "Please Provide Keyspace to Backup"
   sys.exit(1)

# Get CASSANDRA_HOME
#try:
#	CASSANDRA_HOME = os.environ['CASSANDRA_HOME']
#except KeyError:
#   print "Please set the environment variable CASSANDRA_HOME"
#   sys.exit(1)
CASSANDRA_HOME = config.cassandraHome

# Get CASSANDRA_DATA_DIR
def CASSANDRA_DATA_DIR():
#	CASSANDRA_HOME = os.environ['CASSANDRA_HOME']
	cassandraConfigFile =  CASSANDRA_HOME+"/conf/cassandra.yaml"
	
	with open(cassandraConfigFile, 'r') as f:
		keys = yaml.load(f)

	dataFileDirectory = keys["data_file_directories"]
        return dataFileDirectory[0]
	

#CASSANDRA_HOME = os.environ['CASSANDRA_HOME']

NODETOOL = CASSANDRA_HOME +'/bin/nodetool'
# Snapshot format
SNAPSHOTS = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')

# Create snapshots for all keyspaces
print 'Creating Snapshots For ' + KEYSPACE + ' at ' + SNAPSHOTS + '..........'
call([NODETOOL, "snapshot", "-t", SNAPSHOTS, KEYSPACE])

# Get Snapshots Lists
SNAPSHOTS_DIR_LIST = CASSANDRA_DATA_DIR()+"/"+KEYSPACE+"/*/snapshots/"+SNAPSHOTS
print SNAPSHOTS_DIR_LIST

keyspacePath = CASSANDRA_DATA_DIR()+"/"+KEYSPACE

snapshotDirColumnFamilyPaths = glob(SNAPSHOTS_DIR_LIST)
# Get Current working directory
PATH = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory


nodeS3Path = "s3://"+config.bucket_name+"/"+config.node_name
for snapshotDirColumnFamilyPath in snapshotDirColumnFamilyPaths:
	keyspacePath=snapshotDirColumnFamilyPath.split("/snapshots")[0]
        b=keyspacePath.split(CASSANDRA_DATA_DIR())[1]
#	keyspace=b.split("/")[1]
	columnFamily=b.split("/")[2]

	s3SyncDir = nodeS3Path + "/" + config.sync_dir + "/"+KEYSPACE + "/"+columnFamily

	s3SyncCommand = PATH+"/boto-rsync.py "+snapshotDirColumnFamilyPath+" " + s3SyncDir

	s3SyncMetaInfo = SNAPSHOTS + " " + KEYSPACE + " " + nodeS3Path + "/"+config.sync_dir + "/"

	with open("metadata", "a") as myfile:
		myfile.write(s3SyncMetaInfo + "\n")

	print "Syncing Differential Snapshot: <Local-2-S3>"
	print "Executing: " + s3SyncCommand + " to sync snapshot of keyspace " + KEYSPACE + " for cloumn family " + columnFamily
	os.system(s3SyncCommand)


	s3SnapshotDirectory = nodeS3Path + "/snapshots/"+KEYSPACE+"/"+SNAPSHOTS+"/"+columnFamily
	s3RemoteCopyCommand = PATH+"/boto-rsync.py " + s3SyncDir + " " + s3SnapshotDirectory

        metaFileUpdateCommand = PATH + "/boto-rsync.py metadata " + nodeS3Path + "/snapshots/" + KEYSPACE + "/metadata"

        print "Creating Snapshot: <S3-3-S3>"
	print "Executing s3 remote copy command : " + s3RemoteCopyCommand
	print "Executing Metadata upload command : " + metaFileUpdateCommand

        os.system(s3RemoteCopyCommand)
        os.system(metaFileUpdateCommand)	
