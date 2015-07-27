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

# Get Keyspace
KEYSPACE = sys.argv[1]

if KEYSPACE == "":
   print "Please Provide Keyspace to Incremental Backup"
   sys.exit(1)

cassandraHome = config.cassandraHome

# Get cassandraDataDir
def cassandraDataDir():
	cassandraConfigFile =  cassandraHome+"/conf/cassandra.yaml"
	with open(cassandraConfigFile, 'r') as f:
		keys = yaml.load(f)
	dataFileDirectory = keys["data_file_directories"]
        return dataFileDirectory[0]
	
NODETOOL = cassandraHome +'/bin/nodetool'
# timeStamp format
timeStamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H')

# Create Incremental Backup for keyspaces
print 'Creating Incremental Backup For ' + KEYSPACE + '..........'
call([NODETOOL, "flush", KEYSPACE])

# Get Incremental Backup Lists
incrementalBackUpList = cassandraDataDir()+"/"+KEYSPACE+"/*/backups/"
print incrementalBackUpList
keyspacePath = cassandraDataDir()+"/"+KEYSPACE
incrementalBackUpColumnFamilyPaths = glob(incrementalBackUpList)
# Get Current working directory
PATH = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory

nodeS3Path = "s3://"+config.bucket_name+"/"+config.node_name
for incrementalColumnFamilyPath in incrementalBackUpColumnFamilyPaths:
	keyspacePath=incrementalColumnFamilyPath.split("/backups")[0]
	b=keyspacePath.split(cassandraDataDir())[1]

	columnFamily=b.split("/")[2]
	s3SyncDir = nodeS3Path + "/" + config.sync_dir + "/"+KEYSPACE + "/incrementalBackup/"+columnFamily
	s3SyncCommand = PATH+"/boto-rsync.py "+incrementalColumnFamilyPath+" " + s3SyncDir
	
	s3SyncMetaInfo = timeStamp + " " + KEYSPACE + " " + nodeS3Path + "/"+config.sync_dir + "/"
	
	with open("metadata", "a") as myfile:
		myfile.write(s3SyncMetaInfo + "\n")
	print "Syncing Differential incrementalBackup: <Local-2-S3>"
	print "Executing: " + s3SyncCommand + " to sync incrementalBackup of keyspace " + KEYSPACE + " for cloumn family " + columnFamily
	os.system(s3SyncCommand)

	s3incrementalBackupDirectory = nodeS3Path + "/incrementalBackup/"+KEYSPACE+"/"+timeStamp+"/"+columnFamily
	s3RemoteCopyCommand = PATH+"/boto-rsync.py " + s3SyncDir + " " + s3incrementalBackupDirectory

    	metaFileUpdateCommand = PATH + "/boto-rsync.py metadata " + nodeS3Path + "/incrementalBackup/" + KEYSPACE + "/metadata"

    	print "Creating incrementalBackup: <S3-2-S3>"
	print "Executing s3 remote copy command : " + s3RemoteCopyCommand
	print "Executing Metadata upload command : " + metaFileUpdateCommand
	os.system(s3RemoteCopyCommand)
    	os.system(metaFileUpdateCommand)
