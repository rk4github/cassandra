#!/usr/bin/python

from os import listdir
from os.path import isfile, join
import hashlib

def getHexaMd5Checksum(filename):
    md5 = hashlib.md5()
    with open(filename,'rb') as f: 
        for chunk in iter(lambda: f.read(8192), b''): 
            md5.update(chunk)
    return md5.hexdigest()


def getFilesList(parentDirectory):
	files = [ fileName for fileName in listdir(parentDirectory) if isfile(join(parentDirectory,fileName)) ]
	return files


def generateHexzMD5ChecksumForFilesInFolder(parentDirectory):
	files = getFilesList(parentDirectory)
	for fileName in files:
		hexaMD5Checksum = getHexaMd5Checksum(join(parentDirectory,fileName))
		print "%s,%s"%(fileName,hexaMD5Checksum)
