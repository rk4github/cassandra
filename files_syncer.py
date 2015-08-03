#!/usr/bin/python
import hashlib
from files_function import getFilesList
import sets

def getHexaMd5Checksum(stringValue):
	md5 = hashlib.md5()
	md5.update(stringValue)
	return md5.hexdigest()

def getMasterFilesListPath(keySpace):
	masterFilesListPath = "/var/opstree/"+ keySpace +"masterfileslist.meta"
	return masterFilesListPath

#I'll get the list of all the files present in sourceFolder
def getCurrentFilesList(sourceFolder):
	currentFilesList = getFilesList(sourceFolder)
	return currentFilesList

#I'll provide master list of files corresponding to sourceFolder
def getMasterFilesList(sourceFolder,keySpace):
	masterFilesListPath = getMasterFilesListPath(keySpace)
	file = open(masterFilesListPath, 'r')

	fileNames = []
	for fileName in file:
		#print fileName,
		fileNames.append(fileName.rstrip())
	return fileNames
	

#I'll update the master list of files corresponding to source folder
def updateMasterFilesList(sourceFolder,keySpace ):
	currentFilesList = getCurrentFilesList(sourceFolder)
	masterFilesListPath = getMasterFilesListPath(keySpace)
	print "Re creating masterFilesListPath: " + masterFilesListPath
	mastersFileList = open(masterFilesListPath, 'w')
	for fileName in currentFilesList:
		mastersFileList.write(fileName + "\n")
	mastersFileList.close()

def getListOfDeletedFiles(sourceFolder,keySpace):
	return set(getMasterFilesList(sourceFolder, keySpace)) - set(getCurrentFilesList(sourceFolder))

def getNewlyAddedFiles(sourceFolder,keySpace):
	return set(getCurrentFilesList(sourceFolder)) - set(getMasterFilesList(sourceFolder, keySpace))

#sourceFolder = '/var/lib/cassandra/data/demo2/location/snapshots/1438427551883'
#updateMasterFilesList(sourceFolder )

#for files in getNewlyAddedFiles(sourceFolder):
#	print (files)




