import sys, os
import datetime
import re
import yaml
from boto.s3.connection import S3Connection
import config
import inspect


# Get CASSANDRA_HOME
def getCassandraHome():
	cassandra_home=os.environ['CASSANDRA_HOME']
	#cassandra_home=config.cassandraHome
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
	
	# Warning Message for Stopping Cassandra, Once User confirm with Y script will continue futher operation
	length=len(sys.argv)
	if ( length <=3 ):
		userInput = raw_input("Do you want to continue [Y/n]?")
		if userInput == "Y":
            		print("Cassandra Stopped")
        	else:
           		sys.exit(1)
	os.system("pgrep -f cassandra | xargs kill")
	
	commitLogsLocation = "rm -rf " + cassandraDataDir+ "/../commitlog/*"
	print "Deleting commit logs from " + commitLogsLocation

	# Warning Message for commitlog, Once User confirm with Y script will continue futher operation
	if ( length <=3 ):	
		userInput = raw_input("Do you want to continue [Y/n]?")
		if userInput == "Y":
	    		print("Deleted commitlog")
		else:
	   		sys.exit(1)
	os.system(commitLogsLocation)
	
	keySpaceSSTLocation=cassandraDataDir+"/"+keyspaceName
	print "Deleting Key Space SST table from " + keySpaceSSTLocation
	deletSSTTablesCommand = "rm -rf " + keySpaceSSTLocation
	
	# Warning Message for Removing Keyspace directory, Once User confirm with Y script will continue futher operation
        if ( length <=3 ):
		userInput = raw_input("Do you want to continue [Y/n]?")
        	if userInput == "Y":
            		print("Deleted Key Space SST table")
        	else:
           		sys.exit(1)
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
