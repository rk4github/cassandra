# Hecuba
Hecuba is a highly optimized utility that takes care of multi node backup and restore using Amazon S3 as the storage container.

## Features
1. Take full snapshot - Passes only the incremental changes to S3, reduces the data transfer over network.
2. Take incremental backups - Supports incremental backups and again reduces data tranfer by selectively transferring diffs.
3. Restore - Can restore to any given time using the full snapshots and relevant incremental backup.
4. List all available snapshots.

This utility is designed to handle huge database sizes (and has beed tested to the tune of 500 GB). Utility works to minimize load on system & for faster result.

#### Prerequisites
```bash
# Install python
$ apt-get install python
# Install python-pip
$ apt-get install python-pip
# Install yaml
$ pip install yaml
# Install boto
$ pip install boto
# Install AWS-CLI (bundled installer)
$ wget https://s3.amazonaws.com/aws-cli/awscli-bundle.zip
$ unzip awscli-bundle.zip
$ sudo ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
$ aws --version
```

## Getting Started 

### Snapshot :
Hecuba takes `Keyspace` as user input & triggers `Snapshot` against provided `Keyspace`. 

Following arguments are needed to trigger full snapshot 

1. Keyspace name

2. IPs of nodes for which the backup needs to be triggered (IP list needs to be provided as user input and has to be in quotes and comma separated).

```bash
# Trigger Backup ( i.e. <hecuba.py> <backup> <Keyspace> )
$ python hecuba.py backup demo

Note : nodes log can be found at <nodes>/root/backup.log
```

### Incremental Backup : 

To trigger incremental backup following inputs are needed

1. Keyspace name

2. IPs of nodes for which the backup needs to be triggered (IP list needs to be provided as user input and has to be in quotes and comma separated).

```bash
# Trigger Backup ( i.e. <hecuba.py> <backup> <Keyspace> )
$ python hecuba.py incremental  demo

Note : nodes log can be found at <nodes>/root/backup.log
```

### Restore :
To trigger resotre following inputs are needed  

1. Keyspace name

2. IPs of nodes for which the backup needs to be triggered (IP list needs to be provided as user input and has to be in quotes and comma separated)

3. Date (restore point)

```bash
# Trigger Backup ( i.e. <script> <restore> <Keyspace> <restore_point>)
$ python hecuba.py restore demo 20150810

Note : nodes log can be found at <nodes>/root/restore.log
``` 


### List Available Snapshots

It lists all available snapshots

```bash
# List Snapshots ( i.e. <getListOfAvailableSnapshots.py> <Bucketname> <Keyspace> )
$ python getListOfAvailableSnapshots.py cassandra-backup-dir demo
``` 


