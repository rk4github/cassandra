#!/usr/bin/python

import sys
import os
import ConfigParser


def createBackup(ipAddress,absoluteScriptPath):
    command = "source /etc/profile;taskset -c 0 nice -n 19 ionice -c3 -n7 nohup python "+absoluteScriptPath+" "+ sys.argv[2] + " </dev/null >backup.log 2>&1 &"
    ssh_cmd = "ssh "+user+'@'+ipAddress+" "+'"%s"'%command
    scpCommand = "scp backup.py files_syncer.py files_function.py findRestoreSnapshot.py  " +user+'@'+ ipAddress + ":/tmp/"
    print "Copying the relevant scripts to Target machine using: "+scpCommand+" On Machine with ip : "+user+'@'+ipAddress
    os.system(scpCommand)
    print "Exeuting command : " + ssh_cmd
    os.system(ssh_cmd)

def createIncrementalBackup(ipAddress,absoluteScriptPath):
    command = "source /etc/profile;taskset -c 0 nice -n 19 ionice -c3 -n7 nohup python "+absoluteScriptPath+" "+ sys.argv[2] + " </dev/null >incremental.log 2>&1 &"
    ssh_cmd = "ssh "+user+'@'+ipAddress+" "+'"%s"'%command
    scpCommand = "scp incrementalBackup.py files_syncer.py files_function.py " +user+'@'+ ipAddress + ":/tmp/"
    print "Copying the relevant scripts to Target machine using: "+scpCommand+" On Machine with ip : "+user+'@'+ipAddress
    os.system(scpCommand)
    print "Exeuting command : " + ssh_cmd
    os.system(ssh_cmd)

def restore(ipAddress,absoluteScriptPath):
    print "If you proceed further the Keyspace provided will be restored on all provided nodes, following actions will be taken: 1. Stop Cassandra, 2. Delete commit logs, 3. Restore keyspace 4. Restart cassandra."
    userInput = raw_input("Do you want to continue Y/N")
    if userInput == "Y":
        command="source /etc/profile;taskset -c 0 nice -n 19 ionice -c3 -n7 python "+absoluteScriptPath+" "+ sys.argv[2] +" "+ sys.argv[3] +" "+userInput+" </dev/null >restore.log 2>&1 &"
        ssh_cmd="ssh "+user+'@'+ipAddress+" "+'"%s"'%command
        scpCommand = "scp restore_script.py " +user+'@'+ ipAddress + ":/tmp/"
        print  "Copying relevant scripts to Target machine using: "+scpCommand+" On Machine with ip : "+user+'@'+ipAddress
        os.system(scpCommand)
        print "Exeuting command : " + ssh_cmd
        os.system(ssh_cmd)
    else:
        sys.exit(1)    

def createMysqlDump(ipAddress):
    command = "nohup python "+absoluteScriptPath+" "+sys.argv[2]+" "+sys.argv[3]+" "+sys.argv[4]+" </dev/null >mysqlBackup.log 2>&1 &"
    ssh_cmd = "ssh "+user+'@'+ipAddress+" "+'"%s"'%command
    scpCommand = "scp mysqlBackup.py " +user+'@'+ ipAddress + ":/tmp/"
    print "Copying the relevant scripts to Target machine using: "+scpCommand+" On Machine with ip : "+user+'@'+ipAddress
    os.system(scpCommand)
    print "Exeuting command : " + ssh_cmd
    os.system(ssh_cmd)
        
def performAction(inputListOfIPAddress,action,absoluteScriptPath):
    for ipAddress in inputListOfIPAddress.split(','):
    # Conditional execution for Snapshots, Incremental Backup & Restore
        if action == 'backup':
            createBackup(ipAddress,absoluteScriptPath)
        else:
            if action == 'incremental':
                createIncrementalBackup(ipAddress,absoluteScriptPath)
            else:
                if action == 'restore':
                    restore(ipAddress,absoluteScriptPath)
                else:
                    if action == 'mysql':
                        createMysqlDump(ipAddress)             
                    else:
                        print "Please provide valid action to perform: " + action    


#User Interaction and script execution begins from here
action=sys.argv[1]
config = ConfigParser.ConfigParser()
config.readfp(open(r'server.properties'))
listOfIPAddress = config.get('servers', 'ip').split(",")
absoluteScriptPath = config.get('servers', action)
user = config.get('servers', 'user')
print "List of Nodes"
print listOfIPAddress

inputListOfIPAddress = input('Please provide nodes list comma separated values in following format:  "10.24.1.20,10.24.1.21"\n')
performAction(inputListOfIPAddress,action,absoluteScriptPath)

