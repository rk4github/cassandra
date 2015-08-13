# Hecuba
A utility to backup cassandra node using snapshots & incremental backups to Amazon S3.

The objective of the project to make DBA, system admin & DevOps life easier into `Snapshotting`, `Incremental Backup` and `Restoring` cassandra.

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

### Cassandra Backup :
##### Single-node
[backup.py] - Backup script will take `Keyspace` as user input & triggers `Snapshot` against provided `Keyspace`. Once snapshot is created, backup script uses [files_function.py] & [files_syncer.py] for getting differentials files from last `Snapshot` to current `Snapshot`, Newly added files uploaded to S3 & deleted files removed from S3. Here `aws-cli` is being used for uploading files.

```bash
# Trigger Backup ( i.e. <script> <keyspace> )
$ python backup.py demo
```

##### Remote

[wrapper_remote_backup_script.py] - Remote backup script also takes `Keyspace` as user input & triggers `Snapshot` against provided `Keyspace` but will show list of available nodes on which backup will be triggers, user need to choose IP(s) from shown list or add any not provided node IP but those nodes require password-less login & prerequisites to be install otherwise backup fail. Once backup triggered, wrapper script copy require scripts on those nodes and triggers backup in parallel.



```bash
# Trigger Backup ( i.e. <script> <backup> <Keyspace> )
$ python wrapper_remote_backup_script.py backup demo

Note : nodes log can be found at <nodes>/root/backup.log
```


### Cassandra Restore :
##### Single-node
[restore_script.py] - Restore script will take `Keyspace` & `Restore_Point` as user input & user confirmation for stopping, deleting keyspace, after confirmation, triggers restore against provided Keyspace. Here aws-cli is being used for downloading files.

```bash
# Trigger Restore ( i.e. <script> <keyspace> <restore_point> )
$ python backup.py demo 20150810
```

[wrapper_remote_restore_script.py] - Remote restore script also takes `Keyspace` & `Restore_Point` as user input but will show list of available nodes on which restore will be triggers, user need to choose IP(s) from shown list or add any not provided IP but those nodes require password-less login & prerequisites to be install otherwise restore fail. Restore need user confirmation for stopping, deleting keyspace, after confirmation, script triggers restore against provided Keyspace. Here `aws-cli` is being used for downloading files.

```bash
# Trigger Backup ( i.e. <script> <restore> <Keyspace> <restore_point>)
$ python wrapper_remote_backup_script.py restore demo 20150810

Note : nodes log can be found at <nodes>/root/restore.log
```



