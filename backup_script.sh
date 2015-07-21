#!/bin/bash

function backup (){

# Set CASSANDRA_HOME
CASSANDRA_HOME=`pgrep -f cassandra | xargs pwdx |awk '{print $2}'`

# Set DATA_DIR
DATA_DIR=`egrep -v '^$|^#' $CASSANDRA_HOME/conf/cassandra.yaml | awk '/data_file_directories/{getline; print}' |awk '{print $2}'`
# Set BACKUP_DIR
BACKUP_DIR=/backup
NODETOOL=nodetool

# Do not edit below given variable 
TODAY_DATE=$(date +%F)
SNAPSHOT_NAME=$(date +%F%H%M%S)

# Create snapshots for all keyspaces
echo "creating snapshots for all keyspaces ....."
$NODETOOL snapshot -t $SNAPSHOT_NAME

SNAPSHOT_DIR_LIST=`find $DATA_DIR -type d -name $SNAPSHOT_NAME > snp_dir_list`
S3_DIR_LIST=`find $DATA_DIR -type d -name snapshots|awk '{gsub("'$DATA_DIR'", "");print}' | cut -d "/" -f 1,2,3 > snapshot_dir_list`
i=0
for SNP_VAR in `cat snp_dir_list`;
do
i=$((i+1))
dir_list=`sed -n ${i}p snapshot_dir_list`
aws s3 sync "$SNP_VAR" s3://cassandra-backup-dir/sync_dir"${dir_list}" --delete
done
aws s3 sync s3://cassandra-backup-dir/sync_dir s3://cassandra-backup-dir/snapshots/$(date +%Y%m%d%H)
}
backup
