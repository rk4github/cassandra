#!/bin/bash

# The backup script will complete the backup in 2 Stages 
# First Stage: Taking backup of Keyspace SCHEMA
# Second Stage: Taking snapshot of keyspaces


BACKUP_DIR=$1
DATA_DIR=$2
#BACKUP_DIR=/backup
#DATA_DIR=/var/lib/cassandra/data
NODETOOL=nodetool

# Do not edit below given variable 

TODAY_DATE=$(date +%F)
BACKUP_SNAPSHOT_DIR="$BACKUP_DIR/$TODAY_DATE/SNAPSHOTS"
BACKUP_SCHEMA_DIR="$BACKUP_DIR/$TODAY_DATE/SCHEMA"
SNAPSHOT_DIR=$(find $DATA_DIR -type d -name snapshots)
SNAPSHOT_NAME=snp-$(date +%F%H%M%S)
DATE_SCHEMA=$(date +%F%H%M%S)

# Create/Check  BACKUP_DIR 

if [ -d  "$BACKUP_SCHEMA_DIR" ]
then
echo "$BACKUP_SCHEMA_DIR already exist"
else
mkdir -p "$BACKUP_SCHEMA_DIR"
fi

if [ -d  "$BACKUP_SNAPSHOT_DIR" ]
then
echo "$BACKUP_SNAPSHOT_DIR already exist"
else
mkdir -p "$BACKUP_SNAPSHOT_DIR"
fi



# First Stage : SCHEMA BACKUP 

# List all Keyspaces
cqlsh -e "DESC KEYSPACES" |perl -pe 's/\e([^\[\]]|\[.*?[a-zA-Z]|\].*?\a)//g' | sed '/^$/d' > Keyspace_name_schema.cql

#KEYSPACE_NAME=$(cat Keyspace_name_schema.cql)

# Create directory inside backup SCHEMA directory. As per keyspace name.
for i in $(cat Keyspace_name_schema.cql)
do
if [ -d $i ]
then
echo "$i directory exist"
else
mkdir -p $BACKUP_SCHEMA_DIR/$i
fi
done

# Take SCHEMA Backup - All Keyspace and All tables
for VAR_KEYSPACE in $(cat Keyspace_name_schema.cql)
do
cqlsh -e "DESC KEYSPACE  $VAR_KEYSPACE" > "$BACKUP_SCHEMA_DIR/$VAR_KEYSPACE/$VAR_KEYSPACE"_schema-"$DATE_SCHEMA".cql 
done


# Second Stage: Create snapshots for all keyspaces
echo "creating snapshots for all keyspaces ....."
$NODETOOL snapshot -t $SNAPSHOT_NAME

# Get Snapshot directory path
# SNAPSHOT_DIR_LIST=`find $DATA_DIR -type d -name snapshots|awk '{gsub("'$DATA_DIR'", "");print}' > snapshot_dir_list`
SNAPSHOT_DIR_LIST=`find $DATA_DIR -type d -name snapshots|awk '{gsub("'$DATA_DIR'", "");print}' | cut -d "/" -f 1,2,3 > snapshot_dir_list`
# SNAPSHOT_DIR_LIST=`find /var/lib/cassandra/data -type d -name snapshots|awk '{gsub("'/var/lib/cassandra/data'", "");print}' | cut -d "/" -f 1,2,3 > snapshot_dir_list`


# Create directory inside backup directory. As per keyspace name.
for i in `cat snapshot_dir_list`
do
if [ -d $BACKUP_SNAPSHOT_DIR/$SNAPSHOT_NAME/$i ]
then
echo "$i directory exist"
else
# mkdir -p $BACKUP_SNAPSHOT_DIR/$i
mkdir -p $BACKUP_SNAPSHOT_DIR/$SNAPSHOT_NAME/$i
echo $i Directory is created
fi
done

# Copy default Snapshot dir to backup dir

find $DATA_DIR -type d -name $SNAPSHOT_NAME > snp_dir_list
# find $BACKUP_DIR -type d -name $SNAPSHOT_NAME > snp2_dir_list


for SNP_VAR in `cat snp_dir_list`;
do
# Triming DATA_DIR
# SNP_PATH_TRIM=`echo $SNP_VAR|awk '{gsub("'$DATA_DIR'", "");print}'`
SNP_PATH_TRIM=`echo $SNP_VAR|awk '{gsub("'$DATA_DIR'", "");print}' |cut -d "/" -f 1,2,3`
# SNP_PATH_TRIM=`echo /var/lib/cassandra/data/demo/users/snapshots/snp-2015-07-17173252|awk '{gsub("'/var/lib/cassandra/data'", "");print}'|cut -d "/" -f 1,2,3`

#cp -prvf "$SNP_VAR" "$BACKUP_SNAPSHOT_DIR$SNP_PATH_TRIM";
# cp -prf "$SNP_VAR" "$BACKUP_SNAPSHOT_DIR$SNP_PATH_TRIM";
cp -prf "$SNP_VAR/." "$BACKUP_SNAPSHOT_DIR/$SNAPSHOT_NAME$SNP_PATH_TRIM";
done

aws s3 sync $BACKUP_SNAPSHOT_DIR/$SNAPSHOT_NAME s3://cassandra-backup-dir/sync_dir --delete
aws s3 sync s3://cassandra-backup-dir/sync_dir s3://cassandra-backup-dir/snapshots/$(date +%Y%m%d)

# Remove unuse files 
#rm -f Keyspace_name_schema.cql
#rm -f snapshot_dir_list
#rm -f snp_dir_list
