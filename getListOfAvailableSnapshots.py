#!/usr/bin/python
import socket
import sys
from findRestoreSnapshot import getListOfRestorePoint

def getListOfAvailableSnapshots():
	bucketName = sys.argv[1] 
	nodeName = (socket.gethostname())
	nodeS3Path = "s3://" + bucketName + "/" + nodeName
	keySpace = sys.argv[2]
	print getListOfRestorePoint(nodeS3Path,keySpace)

getListOfAvailableSnapshots()
