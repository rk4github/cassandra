import sys, os
import datetime
import re
import yaml
from boto.s3.connection import S3Connection
import config
import inspect


def getRestoreFolder(restoreDate):
		s3KeySpaceRestoreFolder = "s3://" + config.bucket_name +"/snapshots/"+keyspaceName+"/";
		#listCommand = "ls" + restoreDate
		conn = S3Connection('XXXXXXXXXXX','YYYYYYYYYYYY')
		bucket = conn.get_bucket(config.bucket_name)
		for key in bucket.list("re"):
			print key.name.encode('utf-8')

# Get CASSANDRA_HOME
def getCassandraHome():
	cassandra_home=os.environ['CASSANDRA_HOME']
	if cassandra_home != "":
		 return cassandra_home
	else :
		print "Please set the environment variable CASSANDRA_HOME"
		sys.exit(1)
		
def getCassandraDataDir(cassandra_home):
        cassandraConfigFile =  cassandra_home+ "/conf/cassandra.yaml"
        with open(cassandraConfigFile, 'r') as configFile:
                yamlKeyValuePairs = yaml.load(configFile)
        dataFileDirectory = yamlKeyValuePairs["data_file_directories"]
        return dataFileDirectory[0]
		
def restoreKeySpace():
	cassandra_home = getCassandraHome();
	cassandraDataDir = getCassandraDataDir(cassandra_home)
	
	print "Stopping Cassandra..."
	os.system("pgrep -f cassandra | xargs kill")
	
	commitLogsLocation = "rm -rf " + cassandraDataDir+ "/../commitlog/*"
	print "Deleting commit logs from " + commitLogsLocation
	os.system(commitLogsLocation)
	
	keySpaceSSTLocation=cassandraDataDir+"/"+keyspaceName
	print "Deleting Key Space SST table from " + keySpaceSSTLocation
	deletSSTTablesCommand = "rm -rf " + keySpaceSSTLocation
	os.system ( deletSSTTablesCommand)
	
	currentContextFolderPath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory
	s3SnapshotDirectory = "s3://" + config.bucket_name +"/"+config.node_name+"/snapshots/"+keyspaceName+"/"+restoreFolderName+"/"
	s3RemoteDataSyncCommand = currentContextFolderPath+"/boto-rsync.py " + s3SnapshotDirectory + " " + keySpaceSSTLocation
	print "syncing the backed up content in the local Cassandra folder from:"+s3SnapshotDirectory +" to " +keySpaceSSTLocation
	os.system(s3RemoteDataSyncCommand)
	
	print "Starting Cassandra..."
	cassadraStartShellCommand=cassandra_home+"/bin/cassandra"
	os.system(cassadraStartShellCommand);
	

# Get Keyspace
keyspaceName = sys.argv[1]
restoreFolderName = sys.argv[2]
if keyspaceName == "":
   print "Please Provide Keyspace to Restore"
   sys.exit(1)

restoreKeySpace()

	

