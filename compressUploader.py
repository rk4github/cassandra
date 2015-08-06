#!/usr/bin/python
import cStringIO
import gzip
import boto
def sendFileGz(bucket, key, fileName, suffix='.gz'):
    key += suffix
    mpu = bucket.initiate_multipart_upload(key)
    stream = cStringIO.StringIO()
    compressor = gzip.GzipFile(fileobj=stream, mode='w')

    def uploadPart(partCount=[0]):
        partCount[0] += 1
        stream.seek(0)
        mpu.upload_part_from_file(stream, partCount[0])
        stream.seek(0)
        stream.truncate()
    with file(fileName) as inputFile:
        while True:  # until EOF
            chunk = inputFile.read(8192)
            if not chunk:  # EOF?
                compressor.close()
                uploadPart()
                mpu.complete_upload()
                break
            compressor.write(chunk)
            if stream.tell() > 10<<20:  # min size for multipart upload is 5242880
                uploadPart()

connection = boto.connect_s3()
bucketName = connection.get_bucket('cassandra-backup-dir') 
#fileNameAfterUpload = 'README.md'
#fileNameBeforeUpload = 'README.md'
sendFileGz(bucketName,fileNameAfterUpload,fileNameBeforeUpload)

