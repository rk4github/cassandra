# Hecuba
A utility to backup Cassandra node using snapshots & incremental backups to Amazon S3.

The objective of the project to make DBA, system admin & DevOps life easier into `Snapshotting`, `Incremental Backup` and `Restoring` Cassandra.

This utility is made under keeping in mind of huge database (more then 500 GB), first time utility uploads entire snapshot afterwords only upload differential snapshot. Utility works to minimize load on system & for faster result.
This is achieved after using differential calculation, utility compares generated snapshot with available snapshot in s3 (sync_dir) if gets difference upload back to s3, after syncing differences to s3, create snapshot accordingly.

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
Hecuba takes `Keyspace` as user input & triggers `Snapshot` against provided `Keyspace`. Before triggering snapshot ask for list of nodes on which backup will be triggers, user need to provide IP(s), those nodes require password-less login & prerequisites to be install otherwise backup fail. once get list of nodes, utility copy require scripts on those nodes and triggers snapshots in parallel & upload to s3. Here `aws-cli` is being used for uploading files.


```bash
# Trigger Backup ( i.e. <hecuba.py> <backup> <Keyspace> )
$ python hecuba.py backup demo

Note : nodes log can be found at <nodes>/root/backup.log
```

### Incremental Backup : 
Hecuba takes `Keyspace` as user input & triggers `flush` against provided `Keyspace`. Before triggering `flush` ask for list of nodes on which incremental backup will be triggers, user need to provide IP(s), those nodes require password-less login & prerequisites to be install otherwise incremental backup fail. once get list of nodes, utility copy require scripts on those nodes and triggers incremental backup in parallel & upload to s3. Here `aws-cli` is being used for uploading files.


```bash
# Trigger Backup ( i.e. <hecuba.py> <backup> <Keyspace> )
$ python hecuba.py incremental  demo

Note : nodes log can be found at <nodes>/root/backup.log
```



### Restore :
Hecuba takes `Keyspace` & `Restore_Point` as user input & ask for list of nodes on which restore will be triggers, user need to provide IP(s), those nodes require password-less login & prerequisites to be install otherwise restore fail. Restore need user confirmation for stopping cassandra, deleting keyspace, after confirmation, script triggers restore against provided Keyspace. `Restore_point` will match with closest snapshot + incremental Backup. Here `aws-cli` is being used for downloading files.

```bash
# Trigger Backup ( i.e. <script> <restore> <Keyspace> <restore_point>)
$ python hecuba.py restore demo 20150810

Note : nodes log can be found at <nodes>/root/restore.log
```

