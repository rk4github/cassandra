#!/bin/bash

function incremental (){

# Set CASSANDRA_HOME
CASSANDRA_HOME=`pgrep -f cassandra | xargs pwdx |awk '{print $2}'`

# Set DATA_DIR
DATA_DIR=`egrep -v '^$|^#' $CASSANDRA_HOME/conf/cassandra.yaml | awk '/data_file_directories/{getline; print}' |awk '{print $2}'`

NODETOOL=nodetool

# Create incremental backup for all keyspaces
echo "Creating Incremental Backup....."
$NODETOOL flush

INCREMENAL_DIR_LIST=`find $DATA_DIR -type d -name backups > inc_dir_list`
S3_DIR_LIST=`find $DATA_DIR -type d -name snapshots|awk '{gsub("'$DATA_DIR'", "");print}' | cut -d "/" -f 1,2,3 > increment_dir_list`
i=0
for SNP_VAR in `cat inc_dir_list`;
do
i=$((i+1))
dir_list=`sed -n ${i}p increment_dir_list`
aws s3 sync "$SNP_VAR" s3://cassandra-backup-dir/sync_dir_incremantal"${dir_list}" --delete
done
}
incremental
