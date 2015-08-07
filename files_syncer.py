#!/usr/bin/python
import hashlib
from files_function import getFilesList
import sets
import os

def getMasterFilesListPath(nodeS3Path,keySpace,columnFamily):
        masterFilesListPath = "aws s3 ls " + nodeS3Path + "/sync_dir/"+ keySpace + "/" + columnFamily + "/"
        return masterFilesListPath

#I'll get the list of all the files present in sourceFolder
def getCurrentFilesList(sourceFolder):
        currentFilesList = getFilesList(sourceFolder)
        return currentFilesList

#I'll provide master list of files corresponding to sourceFolder
def getMasterFilesList(nodeS3Path,keySpace,columnFamily):
        masterFilesListPath = getMasterFilesListPath(nodeS3Path,keySpace,columnFamily)
        filesInS3SyncDir = os.popen(masterFilesListPath).readlines()
	fileList = []
        for files in filesInS3SyncDir:
                fileName=files.split(' ')
                length = len(fileName)
		fileList.append(fileName[length-1].rstrip())
        return fileList
		
def getListOfDeletedFiles(sourceFolder,keySpace,nodeS3Path,columnFamily):
        return set(getMasterFilesList(nodeS3Path,keySpace,columnFamily)) - set(getCurrentFilesList(sourceFolder))

def getNewlyAddedFiles(sourceFolder,keySpace,nodeS3Path,columnFamily):
        return set(getCurrentFilesList(sourceFolder)) - set(getMasterFilesList(nodeS3Path,keySpace,columnFamily))

