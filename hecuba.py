#!/usr/bin/python

import sys
import os
import ConfigParser

def createBackup():
	command = "source /etc/profile;nohup python "+getAction+" "+ sys.argv[2] + " </dev/null >backup.log 2>&1 &"
	ssh_cmd = "ssh "+ipAddress+" "+'"%s"'%command
	scpCommand = "scp backup.py files_syncer.py files_function.py findRestoreSnapshot.py  " + ipAddress + ":/opt/"
	print scpCommand
	print ssh_cmd
	os.system(scpCommand)
	os.system(ssh_cmd)

def createIncrementalBackup():
	command = "source /etc/profile;nohup python "+getAction+" "+ sys.argv[2] + " </dev/null >incremental.log 2>&1 &"
       	ssh_cmd = "ssh "+ipAddress+" "+'"%s"'%command
        scpCommand = "scp incrementalBackup.py files_syncer.py files_function.py " + ipAddress + ":/opt/"
	print scpCommand
        print ssh_cmd
        os.system(scpCommand)
	os.system(ssh_cmd)

def restore():
	print "If you proceed further the Keyspace provided will be restored on all provided nodes, following actions will be taken: 1. Stop Cassandra, 2. Delete commit logs, 3. Restore keyspace 4. Restart cassandra."
	userInput = raw_input("Do you want to continue Y/N")
	if userInput == "Y":
		command="source /etc/profile;python "+getAction+" "+ sys.argv[2] +" "+ sys.argv[3] +" "+userInput+" </dev/null >restore.log 2>&1 &"
		ssh_cmd="ssh "+ipAddress+" "+'"%s"'%command
		scpCommand = "scp restore_script.py " + ipAddress + ":/opt/"
		print scpCommand
		print ssh_cmd
		os.system(scpCommand)
		os.system(ssh_cmd)
	else:
		sys.exit(1)	

action=sys.argv[1]

config = ConfigParser.ConfigParser()
config.readfp(open(r'server.properties'))
listOfIPAddress = config.get('servers', 'ip').split(",")
getAction = config.get('servers', action)
print "List of Nodes"
print listOfIPAddress

inputListOfIPAddress = input('Please provide nodes list comma separated values of node: i.e. "10.24.1.20,10.24.1.21"\n')

for ipAddress in inputListOfIPAddress.split(','):

	# Conditional execution for Snapshots, Incremental Backup & Restore
	if sys.argv[1] == 'backup':
		createBackup()
	else:
		if sys.argv[1] == 'incremental':
			createIncrementalBackup()
		else:
			if sys.argv[1] == 'restore':
				restore()
			else:
				print "Please provide action to perform"
