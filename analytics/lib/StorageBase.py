#!/usr/bin/env python
import abc
import uuid
import re

from LogHelper import LogHelper

class StorageBase:
    """
    handle activities in object storage systems
    """

    def __init__(self, config, jobId=''):
        """
        constructor
        :param config: i2ap config object
        :param jobId: optional: i2ap job id for logging consistency
        """

        self.config = config
        self.projectId = config['project-id']
        self.storageType = config['storage-type']
        self.bucket = config['stage-bucket']
        self.logName = config['log-name']
        self.logfile = config['log-file']

        self._storeConfig()

        if jobId == '':
            self.jobId = uuid.uuid4()
        else:
            self.jobId = jobId
        self.logger = self.logger = LogHelper.factory(self.config['project-id'], type="filelog", jobId=self.jobId,
                                   destfile=self.logfile)

    @abc.abstractmethod
    def _storeConfig(self):
        """
        storage or type specific connections
        """

    def setBucket(self, bucket):
        """
        set the name of default bucket in the helper
        :param bucket: name of the default bucket
        :return:
        """
        self.bucket = bucket

    @abc.abstractmethod
    def createBucket(self, name):
        """
        creates a new object storage bucket
        :param name: the name of the new bucket to be created
        """

    @abc.abstractmethod
    def checkExist(self, name):
        """
        confirm a bucket exists in the system
        :param name: the name of the bucket to be verified
        :return: boolean
        """

    @abc.abstractmethod
    def deleteBucket(self, name):
        """
        delete an existing bucket from the system
        :param name: the name of the bucket to be deleted
        """

    @abc.abstractmethod
    def uploadFile(self, local, remote):
        """
        uploads a local file into an object storage bucket
        :param local: full path and filename of a file on the local file system
        :param remote: the destination path and filename in object storage
        """

    @abc.abstractmethod
    def downloadFile(self, remote, local):
        """
        downloads a remote object storage file into a local file
        :param remote: the source path and filename, including bucket in object storage
        :param local: the destination path and filename on the local file system
        """

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

    @abc.abstractmethod
    def deleteFile(self, fileName):
        """
        delete a file from a google storage bucket
        :param bucket: the google storage bucket within which to act
        :param fileName: full path and filename to delete
        """

    @abc.abstractmethod
    def moveFile(self, sourceFile, destBucket='', destFile=''):
        """
        move a file from one location to another within google storage
        :param sourceBucket: source google storage bucket
        :param destBucket: destination google storage bucket (could be the same)
        :param fileName: source path and filename (to be moved/renamed)
        """
        if destBucket == '':
            destBucket = self.bucket
        if destFile == '':
            destFile = sourceFile

        try:
            self.copyFile(sourceFile, destBucket, destFile)
        except Exception as gsErr:
            raise RuntimeError(str(gsErr))
        self.deleteFile(sourceFile)

    def copyFolder(self, folder, destBucket):
        """
        copy the contents of files within a bucket to another location with that (or another) bucket
        :param folder
        :param destBucket:
        """
        # grab the contents of the bucket
        fileList = self.getContents()

        for item in fileList:
            self.copyFile(item, destBucket=destBucket)

    def deleteFolder(self, folder):
        """
        delete the contents of a directory within a bucket
        :param folder:
        """
        # grab the contents of the bucket
        fileList = self.getContents(folder=folder)
        for item in fileList:
            self.deleteFile(item)

    @abc.abstractmethod
    def getFile(self, fileName):
        """
        get metadata on a given object out of storage
        :param fileName:
        :return: dictionary
        """

    @abc.abstractmethod
    def getContents(self, folder=''):
        """
        return the contents of a bucket (or folder)
        :param folder:
        :return: list of files
        """

    def checkFileExists(self, fileName):
        """
        Determines if the google storage file exists
        :param remote: the full path name of the file in question
        :return: boolean
        """
        meta = self.getFile(fileName)
        if meta['exists']:
            return True
        else:
            return False

    def getFilesOfPrefix(self, pattern, folder=''):
        """
        get a list of files that match the pattern for a given bucket/folder
        :param folder:
        :param pattern:
        :return: list of file names
        """
        blobs = self.getContents(folder=folder)
        fileList = []
        for obj in blobs:
            fileList.append(obj.name)
        if folder != '':
            pattern = folder + '/' + pattern
        pattern += '.*'
        r = re.compile(pattern)
        prefixList = filter(r.match, fileList)
        return prefixList

    def deleteFilesOfPrefix(self, pattern, folder=''):
        """
        dete a list of files that match the pattern for a given bucket/folder
        :param bucket:
        :param pattern:
        :param folder:
        :return: list of deleted file names
        """
        deleteList = self.getFilesOfPrefix(pattern, folder=folder)
        for obj in deleteList:
            self.deleteFile(obj)
        return deleteList

    def checkFilePatternExists(self, pattern, folder=''):
        """
        check if a pattern exists for the naming of files in a given bucket/folder
        :param pattern:
        :param folder:
        :return:
        """
        patternList = self.getFilesOfPrefix(pattern, folder=folder)
        return len(patternList) > 0
