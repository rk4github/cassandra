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
try:
        os.environ['CASSANDRA_HOME']
except KeyError:
   print "Please set the environment variable CASSANDRA_HOME"
   sys.exit(1)

# Get CASSANDRA_DATA_DIR
def CASSANDRA_DATA_DIR():
	CASSANDRA_HOME = os.environ['CASSANDRA_HOME']
	cassandraConfigFile =  CASSANDRA_HOME+"/conf/cassandra.yaml"
	
	with open(cassandraConfigFile, 'r') as f:
		keys = yaml.load(f)

	dataFileDirectory = keys["data_file_directories"]
        return dataFileDirectory[0]
	

CASSANDRA_HOME = os.environ['CASSANDRA_HOME']

NODETOOL = CASSANDRA_HOME +'/bin/nodetool'
# Snapshot format
SNAPSHOTS = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')

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


for snapshotDirColumnFamilyPath in snapshotDirColumnFamilyPaths:
#	keyspacePath=snapshotDirColumnFamilyPath.split("/snapshots")[0]
#        b=keyspacePath.split(CASSANDRA_DATA_DIR())[1]
#	keyspace=b.split("/")[1]
#	columnFamily=b.split("/")[2]
	columnFamily=ntpath.basename(snapshotDirColumnFamilyPath)

	s3SyncDir = "s3://"+config.bucket_name+"/"+config.node_name+"/"+config.sync_dir+"/"+KEYSPACE+"/"+columnFamily

	s3SyncCommand = PATH+"/boto-rsync.py "+snapshotDirColumnFamilyPath+" " + s3SyncDir

#	s3SyncMetaInfo = SNAPSHOTS+" "+keyspace+" s3://"+config.bucket_name+"/"+config.node_name+"/"+config.sync_dir+"/"

#	with open("metadata", "a") as myfile:
#		myfile.write(s3SyncMetaInfo + "\n")

	print "Syncing Differential Snapshot: <Local-2-S3>"
	print "Executing: " + s3SyncCommand 
	os.system(s3SyncCommand)


	s3SnapshotDirectory = "s3://" + config.bucket_name +"/"+config.node_name+"/snapshots/"+KEYSPACE+"/"+SNAPSHOTS+"/"+columnFamily
	s3RemoteDataSyncCommand = PATH+"/boto-rsync.py " + s3SyncDir + " " + s3SnapshotDirectory

#        metaFileUpdateCommand = PATH+"/boto-rsync.py metadata s3://cassandra-backup-dir/snapshots/"+KEYSPACE+"/metadata"
        print "Creating Snapshot: <S3-3-S3>"
        # print snap
	print "Executing: " + s3RemoteDataSyncCommand
        os.system(s3RemoteDataSyncCommand)
#        os.system(metaFileUpdateCommand)	
