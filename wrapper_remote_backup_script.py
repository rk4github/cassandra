#!/usr/bin/python

import sys
import os
import ConfigParser

action=sys.argv[1]
keyspaceName=sys.argv[2]

config = ConfigParser.ConfigParser()
config.readfp(open(r'server.properties'))
listOfIPAddress = config.get('servers', 'ip').split(",")
getAction = config.get('servers', action)
print "List of Nodes"
print listOfIPAddress
inputListOfIPAddress = input('Please provide nodes list comma separated values of node: i.e. "10.24.1.20,10.24.1.21"\n')

for ipAddress in inputListOfIPAddress.split(','):
	command = "source /etc/profile;nohup python "+getAction+" "+keyspaceName+ " </dev/null >backup.log 2>&1 &"
	ssh_cmd = "ssh "+ipAddress+" "+'"%s"'%command
	scpCommand = "scp backup.py files_syncer.py files_function.py compressUploader.py " + ipAddress + ":/opt/"
	print scpCommand
	print ssh_cmd
	os.system(scpCommand)
	os.system(ssh_cmd)
