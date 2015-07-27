#!/usr/bin/python

import sys
import os
import ConfigParser

action=sys.argv[1]
keyspaceName=sys.argv[2]
restorePoint=sys.argv[3]
config = ConfigParser.ConfigParser()
config.readfp(open(r'server.properties'))
list_of_ipsAddress=config.get('servers', 'ip').split(",")
get_action=config.get('servers', action)

for ipAddress in list_of_ipsAddress:
	command="python "+get_action+" "+keyspaceName+" "+restorePoint
	ssh_cmd="ssh "+ipAddress+" "+'"%s"'%command
	print ssh_cmd
	os.system(ssh_cmd)


