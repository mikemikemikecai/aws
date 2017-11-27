# -*- coding: UTF-8 -*-
#!/usr/bin/env python
import math, os
import boto
from filechunkio import FileChunkIO
import ntpath


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def connAWS(budget_name):
    c = boto.connect_s3()
    b = c.get_bucket(budget_name)
    return b
#Connect to S3


def gci(path):
    '''返回目录下文件名称数组'''
    s = []
    parents = os.listdir(path)
    for parent in parents:
        child = os.path.join(path, parent)
        # print(child)
        if os.path.isdir(child):
            gci(child)
            # print(child)
        else:
            s.append(child)
            # print(child)
    # print(s)
    return s

def uploader(b,sourcePath,destDir):
    '''上传指定文件b:aws连接，sourcePath:文件完整路径'''
# Get file info
# sourcePath = '''/mnt/x/VM/3.Win2012R2_ORACLE12cEPM11124/Windows Server 2012-cl2-s001.vmdk'''
    sourceSize = os.stat(sourcePath).st_size
    destPath = os.path.join(destDir,path_leaf(sourcePath))
    #Create a multipart upload request
    #mp = b.initiate_multipart_upload(os.path.basename(sourcePath))
    mp = b.initiate_multipart_upload(destPath)

    # Use a chunk size of 50 MiB (feel free to change this)
    chunk_size = 52428800
    chunk_count = int(math.ceil(sourceSize / float(chunk_size)))

    # Send the file parts, using FileChunkIO to create a file-like object
    # that points to a certain byte range within the original file. We
    # set bytes to never exceed the original file size.
    for i in range(chunk_count):
        offset = chunk_size * i
        bytes = min(chunk_size, sourceSize - offset)
        with FileChunkIO(sourcePath, 'r', offset=offset, bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)

    # Finish the upload
    mp.complete_upload()


if __name__ == '__main__':
    #print(gci('/mnt/x/VM/3.Win2012R2_ORACLE12cEPM11124'))
    b =  connAWS('huaxinglu')
    #s = gci('/mnt/x/VM/3.Win2012R2_ORACLE12cEPM11124')
    s = gci('/mnt/u/aws_uploader27')
    for i in s:
        uploader(b, i,'win2012')


