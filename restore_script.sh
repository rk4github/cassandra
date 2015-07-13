#!/bin/bash


BACKUP_DIR=$1
DATA_DIR=$2
SNAPSHOTS=$3
# Do not edit below given variable 

TODAY_DATE=$(date +%F)
BACKUP_SNAPSHOT_DIR="$BACKUP_DIR/$TODAY_DATE/SNAPSHOTS"
BACKUP_SCHEMA_DIR="$BACKUP_DIR/$TODAY_DATE/SCHEMA"
SNAPSHOT_DIR=$(find $DATA_DIR -type d -name snapshots)
SNAPSHOT_NAME=snp-$(date +%F-%H%M-%S)
DATE_SCHEMA=$(date +%F-%H%M-%S)

# Available SNAPSHOTS for Keyspaces

#cqlsh -e "DESC KEYSPACES" |perl -pe 's/\e([^\[\]]|\[.*?[a-zA-Z]|\].*?\a)//g' | sed '/^$/d' > Keyspace_name_schema.cql
# Create directory inside backup SCHEMA directory. As per keyspace name.
#i=`cat Keyspace_name_schema.cql |awk '{print $1}'`
#if [ -d $BACKUP_SNAPSHOT_DIR/$i ]
#then
#echo "Available SNAPSHOTS for Keyspaces"
#find $BACKUP_SNAPSHOT_DIR/$i -type d -name snp-$(date +%F)*|awk '{gsub("'$BACKUP_SNAPSHOT_DIR/$i'", "");print}' | cut -d "/" -f4
#else
#echo "SNAPSHOTS not available for Keyspace $i"
#fi
#echo -n "Enter SNAPSHOTS to restore : "
#read SNAPSHOTS
# echo $SNAPSHOTS
#if [ -z "$SNAPSHOTS" ]
#then 
#	echo "Restore Point is not provided" 
#	exit
#else 
#	echo "Restoring SNAPSHOTS $SNAPSHOTS"
#fi

find $DATA_DIR -type d -name $SNAPSHOTS |cut -d "/" -f 1,2,3,4,5,6,7 > restore_dir_list
# find /var/lib/cassandra/data -type d -name snp-2015-07-13-2036-37 |cut -d "/" -f 1,2,3,4,5,6,7 > restore_dir_list
find $BACKUP_SNAPSHOT_DIR -type d -name $SNAPSHOTS > _snapshot_dir_list
#find /backup/2015-07-13/SNAPSHOTS -type d -name snp-2015-07-13-2036-37|awk '{gsub("'$BACKUP_SNAPSHOT_DIR'", "");print}' > _snap_dir_list


# Remove commitlog dir
echo "Removing commitlog"
rm -rf $DATA_DIR/../commitlog/*

# Remove Keyspace 
for i in $(cat Keyspace_name_schema.cql)
do
if [ -d $DATA_DIR/$i ]
then
	echo "Removing $i Keyspace"
	rm -rf $DATA_DIR/$i/*
else
	echo "Keyspace not available"
fi
done

# Copy latest Snapshot dir to data dir
i=0
for SNP_VAR in `cat restore_dir_list`;
do
i=$((i+1))
#SNP_PATH_TRIM=`echo $SNP_VAR|awk '{gsub("'$DATA_DIR'", "");print}'`
#mkdir -p $DATA_DIR$SNP_PATH_TRIM
mkdir -p $SNP_VAR
#cp -prvf "$snapshot_dir_list" "$DATA_DIR$SNP_PATH_TRIM";
value=`sed -n ${i}p _snapshot_dir_list`
cp -prvf "${value}/." "$SNP_VAR";
done 



