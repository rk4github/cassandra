#!/usr/bin/python
import hashlib
from files_function import getFilesList
import sets
import os

def getMasterFilesListPath(nodeS3Path,syncDir,keyspace,columnFamily,action,latestAvailableSnapshotFolder):
	if action == 'backup':
		masterFilesListPath = "aws s3 ls " + nodeS3Path + "/" + syncDir + "/"+ keyspace + "/" + columnFamily + "/"
	else:
		if action == 'incremental':
			masterFilesListPath = "aws s3 ls " + nodeS3Path + "/" + syncDir + "/" + keyspace + "/" + latestAvailableSnapshotFolder + "/" + columnFamily +"/"
		else:
                        print "Please provide valid action to perform: " + action	
        return masterFilesListPath

#I'll get the list of all the files present in sourceFolder
def getCurrentFilesList(sourceFolder):
        currentFilesList = getFilesList(sourceFolder)
        return currentFilesList

#I'll provide master list of files corresponding to sourceFolder
def getMasterFilesList(nodeS3Path,syncDir,keyspace,columnFamily,action,latestAvailableSnapshotFolder):
        masterFilesListPath = getMasterFilesListPath(nodeS3Path,syncDir,keyspace,columnFamily,action,latestAvailableSnapshotFolder)
        filesInS3SyncDir = os.popen(masterFilesListPath).readlines()
	fileList = []
        for files in filesInS3SyncDir:
                fileName=files.split(' ')
                length = len(fileName)
		fileList.append(fileName[length-1].rstrip())
        return fileList
		
def getListOfDeletedFiles(sourceFolder,nodeS3Path,syncDir,keyspace,columnFamily,action,latestAvailableSnapshotFolder):
        return set(getMasterFilesList(nodeS3Path,syncDir,keyspace,columnFamily,action,latestAvailableSnapshotFolder)) - set(getCurrentFilesList(sourceFolder))

def getNewlyAddedFiles(sourceFolder,nodeS3Path,syncDir,keyspace,latestAvailableSnapshotFolder,columnFamily,action):
        return set(getCurrentFilesList(sourceFolder)) - set(getMasterFilesList(nodeS3Path,syncDir,keyspace,columnFamily,action,latestAvailableSnapshotFolder))

