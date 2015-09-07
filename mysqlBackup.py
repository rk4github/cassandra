import os
import socket
import sys
import datetime
import datetime as DT

def createMysqlDump():
    databaseName = sys.argv[3]
    databaseUserName = sys.argv[1]
    databasePassword = sys.argv[2]
    backupTime = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')
    outputFileName = databaseName + backupTime + ".sql"
    commandToCreateMysqlDump = "mysqldump -u"+databaseUserName+ " -p"+databasePassword+ " "+databaseName+ " > " +outputFileName
    print "Creating Mysql Database Dump of Database Name "+databaseName
    print commandToCreateMysqlDump
    os.system(commandToCreateMysqlDump)
    return outputFileName

def getNodeBackupS3Path():
    bucketName = "bidgely-public"
    nodeName = (socket.gethostname())
    nodeS3Path = "s3://"+bucketName+"/"+nodeName
    return nodeS3Path

def copyMysqlDumpToS3():
    outputFileName = createMysqlDump()
    s3SyncDir = getNodeBackupS3Path()
    s3SyncCommand = "aws s3 cp "+ outputFileName + " " + s3SyncDir + "/mysqlbackup/" + outputFileName
    print "Uploading Mysql Dump: <Local-2-S3>"
    print s3SyncCommand
    os.system(s3SyncCommand)

def get7thDayFromCurrentDay():
    today = DT.date.today()
    week_ago = today - DT.timedelta(days=7)
    return week_ago

def getListOfMysqlDumpCreatedTime():
    s3SyncDir = getNodeBackupS3Path()
    commandToGetlistOfMysqlDumpFiles = "aws s3 ls " + s3SyncDir + "/mysqlbackup/"
    listOfMysqlDumpFiles = os.popen(commandToGetlistOfMysqlDumpFiles).readlines()
    return listOfMysqlDumpFiles

def removeMysqlDumpOlderThen7thDay():
    s3SyncDir = getNodeBackupS3Path()
    week_ago = get7thDayFromCurrentDay()
    listOfMysqlDumpFiles = getListOfMysqlDumpCreatedTime()
    for createdTimeOfMysqlDumpAtS3 in listOfMysqlDumpFiles:
        getCreatedDateOfMysqlDumpAtS3 = createdTimeOfMysqlDumpAtS3.rstrip('/').split()[0].strip('/')
        getOlderDateMoreThen7Days = week_ago > datetime.datetime.strptime(getCreatedDateOfMysqlDumpAtS3, '%Y-%m-%d').date()
        if getOlderDateMoreThen7Days == 'true':
            getFileName = createdTimeOfMysqlDumpAtS3.rstrip('/').split()[3].strip('/')
            commandToRemoveOlderMysqlDumpAtS3 = "aws s3 rm " + s3SyncDir + "/mysqlbackup/"+getFileName
            os.system(commandToRemoveOlderMysqlDumpAtS3)


def main():
    copyMysqlDumpToS3()
    removeMysqlDumpOlderThen7thDay()

main()
