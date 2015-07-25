#!/usr/bin/python

import temp
import inspect
# from sys import argv
from subprocess import call
import subprocess
import datetime
from glob import glob
import getopt, sys, os
import boto
from boto.exception import S3ResponseError


nn=""
for KEYSPACE in sys.argv[1:]:
        nn=KEYSPACE+" "+nn
	#print KEYSPACE

# Get CASSANDRA_HOME
try:
        os.environ['CASSANDRA_HOME']
except KeyError:
   print "Please set the environment variable CASSANDRA_HOME"
   sys.exit(1)

# Get CASSANDRA_DATA_DIR
CASSANDRA_HOME = os.environ['CASSANDRA_HOME']
file_name =  CASSANDRA_HOME+"/conf/cassandra.yaml"
with open(file_name, 'r') as f:
    for line in f:
        if line == 'data_file_directories:\n':
            CASSANDRA_DATA_DIR = f.next().strip().split(" ")[1]

NODETOOL = 'nodetool'
# Snapshot format
SNAPSHOTS = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')

# Create snapshots for all keyspaces
print 'Creating Snapshots For All Keyspaces.....'
call([NODETOOL, "snapshot", "-t", SNAPSHOTS, KEYSPACE])

# Get Snapshots Lists
SNAPSHOTS_DIR_LIST = CASSANDRA_DATA_DIR+"/*/*/snapshots/"+SNAPSHOTS
paths = glob(SNAPSHOTS_DIR_LIST)


PATH = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory
cmd = PATH+"/temp.py backup.py s3://cassandra-backup-dir"
os.system(cmd)


for dir_path in paths:
	a=dir_path.split("/snapshots")[0]
        b=a.split(CASSANDRA_DATA_DIR)[1]
	c=b.split("/")[1]
	d=b.split("/")[2]
	with open("property.file", "a") as prop:
                myfile.write( + "\n")
	cmd = PATH+"/temp.py "+dir_path+" s3://cassandra-backup-dir/sync_dir/"+b
	text = SNAPSHOTS+" "+c+" s3://cassandra-backup-dir/sync_dir/"+c
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

	

#snap = PATH+"/temp.py s3://cassandra-backup-dir/sync_dir/"+KEYSPACE+" ""s3://cassandra-backup-dir/snapshots/"+SNAPSHOTS+"/"+KEYSPACE
#meta = PATH+"/temp.py metadata s3://cassandra-backup-dir/snapshots/"+SNAPSHOTS+"/metadata"
#print "Creating Snapshot: <S3-3-S3>"
# print snap
#os.system(snap)
#os.system(meta)


