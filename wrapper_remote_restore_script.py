#!/usr/bin/python

import sys
import os
import ConfigParser

action=sys.argv[1]
keyspaceName=sys.argv[2]
restorePoint=sys.argv[3]
config = ConfigParser.ConfigParser()
config.readfp(open(r'server.properties'))
listOfIPAddress = config.get('servers', 'ip').split(",")
getAction=config.get('servers', action)

print "List of Nodes"
print listOfIPAddress 

inputListOfIPAddress = input('Please provide nodes list comma separated values of node: i.e. "10.24.1.20,10.24.1.21"\n')

print "Stopping Cassandra..."
userInput = raw_input("Do you want to continue [Y/n]?")
if userInput == "Y":
	print("Cassandra Stopped")
else:
        sys.exit(1)
print "Deleting commitlog"
userInput = raw_input("Do you want to continue [Y/n]?")
if userInput == "Y":
        print("Deleted commitlog")
else:
        sys.exit(1)
print "Deleting Key Space SSTtable"
userInput = raw_input("Do you want to continue [Y/n]?")
if userInput == "Y":
        print("Deleted Key Space SST table")
else:
        sys.exit(1)


for ipAddress in inputListOfIPAddress.split(','):
	command="source /etc/profile;python "+getAction+" "+keyspaceName+" "+restorePoint+" "+userInput+" </dev/null >restore.log 2>&1 &"
	ssh_cmd="ssh "+ipAddress+" "+'"%s"'%command
	scpCommand = "scp restore_script.py " + ipAddress + ":/opt/" 
	print scpCommand
	print ssh_cmd
	os.system(scpCommand)
	os.system(ssh_cmd)

