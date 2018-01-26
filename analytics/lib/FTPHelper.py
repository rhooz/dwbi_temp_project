#!/usr/bin/env python
import abc
import re
import pysftp
import uuid
import datetime
from time import strptime
import hashlib

from StorageBase import StorageBase

class FTPHelper(StorageBase):
    """
    handle activities in object storage systems
    """

    def _storeConfig(self):
        """
        storage or type specific connections
        """
        self.client = pysftp.Connection(self.config['storage-project'], username=self.config['storage-user'], password=self.config['storage-password'])

    def setBucket(self, bucket):
        """
        set the name of default bucket in the helper
        :param bucket: name of the default bucket
        :return:
        """
        self.bucket = bucket

    def createBucket(self, name):
        """
        creates a new object storage bucket
        :param name: the name of the new bucket to be created
        """
        try:
            # change diretory to root
            self.client.cd('/')
            # now make a "bucket"
            self.client.mkdir(name, mode=664)
        except Exception as bucketErr:
            raise StandardError("FTPHelper.createBucket: Error creating root level directory. Details: " + str(bucketErr))

    def checkExist(self, name):
        """
        confirm a bucket exists in the system
        :param name: the name of the bucket to be verified
        :return: boolean
        """
        try:
            return self.client.exists(self.bucket)
        except Exception as bucketErr:
            raise RuntimeError(str(bucketErr))

    def deleteBucket(self, name):
        """
        delete an existing bucket from the system
        :param name: the name of the bucket to be deleted
        """
        try:
            # change diretory to root
            self.client.cd('/')
            # remove the directory if it exists
            if self.checkExist(name):
                self.client.rmdir(name)
        except Exception as bucketErr:
            raise RuntimeError(str(bucketErr))

    def uploadFile(self, local, remote):
        """
        uploads a local file into an object storage bucket
        :param local: full path and filename of a file on the local file system
        :param remote: the destination path and filename in object storage
        """
        try:
            # upload the file
            self.client.put(local, remotepath=self.bucket + '/' + remote)
        except Exception as fileErr:
            raise RuntimeError(str(fileErr))

    def downloadFile(self, remote, local):
        """
        downloads a remote object storage file into a local file
        :param remote: the source path and filename, including bucket in object storage
        :param local: the destination path and filename on the local file system
        """
        try:
            # download the file
            self.client.get(self.bucket + '/' + remote, localpath=local)
        except Exception as fileErr:
            raise RuntimeError(str(fileErr))

    @abc.abstractmethod
    def streamFile(self, remote):
        """
        Pull the file as a stream from object storage
        :param remote: the source path and filename, including bucket in google storage
        :return: string (stream)
        """

    @abc.abstractmethod
    def copyFile(self, sourceFile, destBucket='', destFile=''):
        """
        copies a file from one google storage location to another
        :param sourceFile: source path and filename (to be copied)
        :param destBucket: destination google storage bucket (could be the same)
        :param destFile: destination path and filename
        """
        try:
            # there is not remote ftp copy command
            # so, download the file
            tmpName = str(uuid.uuid4()) + '.tmp'
            self.downloadFile(sourceFile, tmpName)
            # upload the file to destination
            if destBucket == '':
                destBucket = self.bucket
            bucketTemp = self.bucket
            self.setBucket(destBucket)
            if destFile == '':
                destFile = sourceFile
            self.uploadFile(tmpName, destFile)
            # restore the default bucket
            self.setBucket(bucketTemp)
            # clean out the temp file
            os.remove(tmpName)
        except Exception as fileErr:
            raise RuntimeError(str(fileErr))

    def deleteFile(self, fileName):
        """
        delete a file from a google storage bucket
        :param bucket: the google storage bucket within which to act
        :param fileName: full path and filename to delete
        """
        if self.checkFileExists(fileName):
            try:
                self.client.remove(self.bucket + '/' + fileName)
            except Exception as fileErr:
                raise RuntimeError(str(fileErr))
        else:
            raise RuntimeError('StorageBase.deleteFile: Reqested File Not Found.')

    def moveFile(self, sourceBucket, destBucket, fileName):
        """
        move a file from one location to another within google storage
        :param sourceBucket: source google storage bucket
        :param destBucket: destination google storage bucket (could be the same)
        :param fileName: source path and filename (to be moved/renamed)
        """
        try:
            self.client.rename(sourceBucket + '/' + fileName, destBucket + '/' + fileName)
        except Exception as fileErr:
            raise RuntimeError(str(fileErr))

    def getFile(self, fileName):
        """
        get metadata on a given object out of storage
        :param fileName:
        :return: dictionary
        """
        meta = {}
        meta['name'] = fileName
        try:
            stat = str(self.client.stat(self.bucket + '/' + fileName)).split()
            meta['size'] = stat[4]
            meta['contentType'] = 'application/octet-stream'
            meta['lastModified'] = str(datetime.datetime.now().year) + '-' + str(strptime(stat[6],'%b').tm_mon).zfill(2) + '-' + stat[5] + ":" + stat[7] + ':00'
            meta['etag'] = hashlib.md5(fileName + str(meta['size'] + str(meta['lastModified']))).hexdigest()
            meta['exists'] = True
        except:
            meta['exists'] = False
        return meta

    def getContents(self, folder=''):
        """
        return the contents of a bucket (or folder)
        :param folder:
        :return: list of files
        """
        try:
            if folder != '':
                checkFolder = self.bucket + '/' + folder
            else:
                checkFolder = self.bucket
            return self.client.listdir(checkFolder)
        except Exception as fileErr:
            raise RuntimeError(str(fileErr))

    def checkFileExists(self, fileName):
        """
        Determines if the google storage file exists
        :param remote: the full path name of the file in question
        :return: boolean
        """
        try:
            return self.client.exists(self.bucket + '/' + fileName)
        except Exception as bucketErr:
            raise RuntimeError(str(bucketErr))
