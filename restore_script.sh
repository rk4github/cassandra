#!/bin/bash

SNAPSHOTS=$1

function restore (){
SNAPSHOTS=$1

# Set CASSANDRA_HOME
CASSANDRA_HOME=`pgrep -f cassandra | xargs pwdx |awk '{print $2}'`

# Set DATA_DIR
DATA_DIR=`egrep -v '^$|^#' $CASSANDRA_HOME/conf/cassandra.yaml | awk '/data_file_directories/{getline; print}' |awk '{print $2}'`

# Remove commitlog dir
echo "Removing commitlog..."
rm -rf $DATA_DIR/../commitlog/*

# Remove Keyspaces
echo "Removing Keyspaces..."
rm -rf $DATA_DIR
echo "Creating Cassandra Data Directories"
mkdir -p $DATA_DIR

# Restore SNAPSHOTS
aws s3 sync s3://cassandra-backup-dir/snapshots/$SNAPSHOTS $DATA_DIR

# Restart CASSANDRA
echo "Stopping Cassandra..."
pgrep -f cassandra | xargs kill
echo "Starting Cassandra..."
bash $CASSANDRA_HOME/bin/cassandra
}
restore $1
