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
import socket
#from files_syncer import updateMasterFilesList
from files_syncer import getNewlyAddedFiles
from files_syncer import getListOfDeletedFiles
from files_syncer import getMasterFilesList

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

# Get cassandraDataDir
def cassandraDataDir():
        cassandraConfigFile =  cassandraHome+"/conf/cassandra.yaml"

        with open(cassandraConfigFile, 'r') as f:
                keys = yaml.load(f)

        dataFileDirectory = keys["data_file_directories"]
        return dataFileDirectory[0]

NODETOOL = cassandraHome +'/bin/nodetool'
# Snapshot format
s3Snapshot = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
snapshot = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')
# Create snapshots for all keyspaces
print 'Creating Snapshots For ' + KEYSPACE + ' at ' + snapshot + '..........'
call([NODETOOL, "snapshot", "-t", snapshot, KEYSPACE])

# Get Snapshots Lists
snapshotsDirList = cassandraDataDir()+"/"+KEYSPACE+"/*/snapshots/"+snapshot
#print snapshotsDirList

keyspacePath = cassandraDataDir()+"/"+KEYSPACE

snapshotDirColumnFamilyPaths = glob(snapshotsDirList)
# Get Current working directory
PATH = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory

bucketName = "cassandra-backup-dir"
nodeName = (socket.gethostname())

nodeS3Path = "s3://"+bucketName+"/"+nodeName

for snapshotDirColumnFamilyPath in snapshotDirColumnFamilyPaths:
        keyspacePath=snapshotDirColumnFamilyPath.split("/snapshots")[0]
        b=keyspacePath.split(cassandraDataDir())[1]
        columnFamily=b.split("/")[2]

        s3SyncDir = nodeS3Path + "/sync_dir/"+KEYSPACE + "/"+columnFamily

        for files in getNewlyAddedFiles(snapshotDirColumnFamilyPath, KEYSPACE,nodeS3Path,columnFamily):
                os.chdir(snapshotDirColumnFamilyPath)
		fileName = files.split('.tar.gz')[0]
                if os.path.isfile(fileName):
                        compressCommand = "tar -czf "+files+" "+fileName
                        os.system(compressCommand)
                s3SyncCommand = "aws s3 cp "+ files + " " + s3SyncDir + "/" + files
                print "Syncing Differential Snapshot: <Local-2-S3>"
                print (files)
                print "Executing: " + s3SyncCommand + " to sync snapshot of keyspace " + KEYSPACE + " for cloumn family " + columnFamily
                removeCompressedFiles = "rm -f "+ files
		os.system(s3SyncCommand)
		os.system(removeCompressedFiles)
		
        for files in getListOfDeletedFiles(snapshotDirColumnFamilyPath, KEYSPACE,nodeS3Path,columnFamily):
                s3RemoveCommand = "aws s3 rm " + s3SyncDir + "/" + files
                print "Removing deleted Files: <From-S3>"
                print (files)
                print "Executing: " + s3RemoveCommand + " to Remove Deleted of Files From " + KEYSPACE + " for cloumn family " + columnFamily
                os.system(s3RemoveCommand)

        os.chdir(PATH)

        s3SnapshotDirectory = nodeS3Path + "/snapshots/"+KEYSPACE+"/"+s3Snapshot+"/"+columnFamily
        s3RemoteCopyCommand = "aws s3 sync " + s3SyncDir + " " + s3SnapshotDirectory

        print "Creating Snapshot: <S3-3-S3>"
        print "Executing s3 remote copy command : " + s3RemoteCopyCommand
        
        os.system(s3RemoteCopyCommand)
        
