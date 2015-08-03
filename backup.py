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
from files_syncer import updateMasterFilesList
from files_syncer import getNewlyAddedFiles
from files_syncer import getListOfDeletedFiles


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
print snapshotsDirList

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


        #s3SyncMetaInfo = snapshot + " " + KEYSPACE + " " + nodeS3Path + "/sync_dir/"

        #with open("metadata", "a") as myfile:
        #        myfile.write(s3SyncMetaInfo + "\n")
		
        for files in getNewlyAddedFiles(snapshotDirColumnFamilyPath, KEYSPACE):
                os.chdir(snapshotDirColumnFamilyPath)

                if os.path.isfile(files):
                        compressCommand = "tar -czf "+files+".tar.gz "+files
                        os.system(compressCommand)
                fileName = files + ".tar.gz"
                s3SyncCommand = "aws s3 cp "+ fileName + " " + s3SyncDir + "/" + fileName
                print "Syncing Differential Snapshot: <Local-2-S3>"
                print (files)
                print "Executing: " + s3SyncCommand + " to sync snapshot of keyspace " + KEYSPACE + " for cloumn family " + columnFamily
                os.system(s3SyncCommand)
		
	for files in getListOfDeletedFiles(snapshotDirColumnFamilyPath, KEYSPACE):
		fileName = files + ".tar.gz"
		s3RemoveCommand = "aws s3 rm " + s3SyncDir + "/" + fileName
		print "Removing deleted Files: <From-S3>"
		print (files)
		print "Executing: " + s3RemoveCommand + " to Remove Deleted of Files From " + KEYSPACE + " for cloumn family " + columnFamily
		os.system(s3RemoveCommand)
				
	updateMasterFilesList(snapshotDirColumnFamilyPath,KEYSPACE )
        os.chdir(PATH)

        s3SnapshotDirectory = nodeS3Path + "/snapshots/"+KEYSPACE+"/"+s3Snapshot+"/"+columnFamily
        s3RemoteCopyCommand = "aws s3 sync " + s3SyncDir + " " + s3SnapshotDirectory

        #metaFileUpdateCommand = PATH + "/boto-rsync.py metadata " + nodeS3Path + "/snapshots/" + KEYSPACE + "/metadata"

        print "Creating Snapshot: <S3-3-S3>"
        print "Executing s3 remote copy command : " + s3RemoteCopyCommand
        #print "Executing Metadata upload command : " + metaFileUpdateCommand

        os.system(s3RemoteCopyCommand)
        #os.system(metaFileUpdateCommand)

