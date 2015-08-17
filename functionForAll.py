#!/usr/bin/python

import os
import sys
import yaml


def getCassandraHome():
	cassandraHome=''
	try:
		cassandraHome = os.environ['CASSANDRA_HOME']
        except KeyError:
		print "Please set the environment variable CASSANDRA_HOME"
		sys.exit(1)
	return cassandraHome

def validateKeyspace(keyspace):
	if keyspace == "":
		print "Please Provide Keyspace to Backup"
		sys.exit(1)

def getCassandraDataDirectory():
	cassandraHome = getCassandraHome()
	cassandraConfigFile =  cassandraHome+"/conf/cassandra.yaml"
	with open(cassandraConfigFile, 'r') as f:
		keys = yaml.load(f)
	dataFileDirectory = keys["data_file_directories"]
	return dataFileDirectory[0]

def userInteractionAndConfirmationForRestoreAction():
	length=len(sys.argv)
	if ( length <=3 ):
        	userInput = raw_input("If you proceed further the Keyspace provided will be restored on node, following actions will be taken: 1. Stop Cassandra, 2. Delete commit logs, 3. Restore keyspace 4. Restart cassandra. Do you want to continue [Y/n]?")
	        if userInput == "Y":
        	    print("Cassandra Stopped")
	        else:
        	    sys.exit(1)
	cassandraDataDir = getCassandraDataDirectory()
	#os.system("pgrep -f cassandra | xargs kill")
	commitLogsLocation = "rm -rf " + cassandraDataDir+ "/../commitlog/*"
	#os.system(commitLogsLocation)
	keySpaceSSTLocation=cassandraDataDir+"/"+keyspace+"/"
	print "Deleting Key Space SST table from " + keySpaceSSTLocation
	deletSSTTablesCommand = "rm -rf " + keySpaceSSTLocation
	#os.system ( deletSSTTablesCommand)
	
	
	
keyspace = sys.argv[1]			
userInteractionAndConfirmationForRestoreAction()










