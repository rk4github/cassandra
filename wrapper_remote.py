#!/usr/bin/python

import sys
import os
import ConfigParser


action=sys.argv[1]

config = ConfigParser.ConfigParser()
config.readfp(open(r'server.properties'))
list_of_ips=config.get('servers', 'ip').split(",")
get_action=config.get('servers', action)

print get_action
new="123"
new1="demo"
for ip in list_of_ips:
	command="python "+get_action+" "+new1+" "+new
	ssh_cmd="ssh vagrant@"+ip+" "+'"%s"'%command
	print ssh_cmd
	os.system(ssh_cmd)


