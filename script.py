#!/usr/bin/python

import temp
import inspect
from subprocess import call
import subprocess
import datetime
from glob import glob
import getopt, sys, os
import boto
from boto.exception import S3ResponseError
import config

# Get Keyspace
KEYSPACE = sys.argv[1:]:
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
	


NODETOOL = 'nodetool'
# Snapshot format
SNAPSHOTS = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')

# Create snapshots for all keyspaces
print 'Creating Snapshots For'+KEYSPACE+'.....'
call([NODETOOL, "snapshot", "-t", SNAPSHOTS, KEYSPACE])

# Get Snapshots Lists
SNAPSHOTS_DIR_LIST = CASSANDRA_DATA_DIR()+"/"+KEYSPACE+"/*/snapshots/"+SNAPSHOTS
print SNAPSHOTS_DIR_LIST
paths = glob(SNAPSHOTS_DIR_LIST)
# Get Current working directory
PATH = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory

for dir_path in paths:
	a=dir_path.split("/snapshots")[0]
        b=a.split(CASSANDRA_DATA_DIR)[1]
	c=b.split("/")[1]
	d=b.split("/")[2]
	cmd = PATH+"/temp.py "+dir_path+" s3://"+config.bucket_name+"/"+config.node_name+"/"+config.sync_dir+"/"
	text = SNAPSHOTS+" "+c+" s3://"+config.bucket_name+"/"+config.node_name+"/"+config.sync_dir+"/"
	with open("metadata", "a") as myfile:
		myfile.write(text + "\n")
	print "Syncing Differential Snapshot: <Local-2-S3>"
	os.system(cmd)
	snap = PATH+"/temp.py s3://cassandra-backup-dir/sync_dir/"+KEYSPACE+"/"+d+ " ""s3://cassandra-backup-dir/snapshots/"+KEYSPACE+"/"+SNAPSHOTS+"/"+d
        meta = PATH+"/temp.py metadata s3://cassandra-backup-dir/snapshots/"+KEYSPACE+"/metadata"
        print "Creating Snapshot: <S3-3-S3>"
        # print snap
        os.system(snap)
        os.system(meta)	
