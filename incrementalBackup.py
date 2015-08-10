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
from files_syncer import getNewlyAddedFiles
from files_syncer import getListOfDeletedFiles
from files_syncer import getMasterFilesList
import socket


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

bucketName = "cassandra-backup-dir"
nodeName = (socket.gethostname())
nodeS3Path = "s3://"+bucketName+"/"+nodeName

for incrementalColumnFamilyPath in incrementalBackUpColumnFamilyPaths:
        keyspacePath=incrementalColumnFamilyPath.split("/backups")[0]
        b=keyspacePath.split(cassandraDataDir())[1]

        columnFamily=b.split("/")[2]
	syncDir = 'incrementalBackupSyncDir'
	incrementalBackupSyncDir = nodeS3Path + "/" + syncDir + "/" + KEYSPACE + "/" + columnFamily
        
	for files in getNewlyAddedFiles(incrementalColumnFamilyPath,syncDir,KEYSPACE,nodeS3Path,columnFamily):
                os.chdir(incrementalColumnFamilyPath)
                s3SyncCommand = "aws s3 cp "+ files + " " + incrementalBackupSyncDir + "/" + files
                print "Syncing Differential Incremental Backup: <Local-2-S3>"
                print (files)
                print "Executing: " + s3SyncCommand + " to sync incremental backup of keyspace " + KEYSPACE + " for cloumn family " + columnFamily
                os.system(s3SyncCommand)
