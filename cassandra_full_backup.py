#!/bin/bash/python

# from sys import argv
from subprocess import call
import sys, os
import subprocess
nn=""
for KEYSPACE in sys.argv[1:]:
        nn=KEYSPACE+" "+nn
		print KEYSPACE		

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

print CASSANDRA_DATA_DIR
NODETOOL = 'nodetool'
# Snapshot format
SNAPSHOTS = subprocess.Popen(["date", "+%Y%m%d%H%M%S"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
SNAPSHOTS, err = SNAPSHOTS.communicate()    															
print SNAPSHOTS

# Create snapshots for all keyspaces
print 'Creating Snapshots For All Keyspaces.....'
call([NODETOOL, "snapshot", "-t", SNAPSHOTS])

