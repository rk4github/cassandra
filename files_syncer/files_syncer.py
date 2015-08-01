#!/usr/bin/python
import hashlib
from files_function import getFilesList

def getHexaMd5Checksum(stringValue):
	md5 = hashlib.md5()
	md5.update(stringValue)
	return md5.hexdigest()

def getMasterFilesListPath():
	masterFilesListPath = "/etc/opstree/masterfileslist.meta"
	return masterFilesListPath

#I'll get the list of all the files present in sourceFolder
def getCurrentFilesList(sourceFolder):
	currentFilesList = getFilesList(sourceFolder)
	return currentFilesList

#I'll provide master list of files corresponding to sourceFolder
def getMasterFilesList(sourceFolder):
	masterFilesListPath = getMasterFilesListPath()
	file = open(masterFilesListPath, 'r')

	fileNames = []
	for fileName in file:
		#print fileName,
		fileNames.append(fileName.rstrip())
	return fileNames
	

#I'll update the master list of files corresponding to source folder
def updateMasterFilesList(sourceFolder ):
	currentFilesList = getCurrentFilesList(sourceFolder)
	masterFilesListPath = getMasterFilesListPath()
	print "Re creating masterFilesListPath: " + masterFilesListPath
	mastersFileList = open(masterFilesListPath, 'w')
	for fileName in currentFilesList:
		mastersFileList.write(fileName + "\n")
	mastersFileList.close()


def getListOfDeletedFiles(sourceFolder):
	return set(getMasterFilesList(sourceFolder)) - set(getCurrentFilesList(sourceFolder))

def getNewlyAddedFiles(sourceFolder):
	return set(getCurrentFilesList(sourceFolder)) - set(getMasterFilesList(sourceFolder))

	
