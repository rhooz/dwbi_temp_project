#!/usr/bin/env python

from minio import Minio
from minio.error import BucketAlreadyExists

from StorageBase import StorageBase

class MIHelper(StorageBase):
    """
    Implementation of the StorageBase interface for Minio Object Storage
    """
    def _storeConfig(self):
        self.client = Minio(self.config['storage-project'], access_key=self.config['storage-key'],
                      secret_key=self.config['storage-secret'], secure=False)

    def createBucket(self, name):
        """
        creates a new object storage bucket
        :param name: the name of the new bucket to be created
        """
        try:
            self.client.make_bucket(name)
        except BucketAlreadyExists as bucketErr:
            pass
        except Exception as bucketErr:
            raise RuntimeError(str(bucketErr))

    def checkExist(self, name):
        """
        confirm a bucket exists in the system
        :param name: the name of the bucket to be verified
        :return: boolean
        """
        try:
            return self.client.bucket_exists(name)
        except Exception as bucketErr:
            raise RuntimeError(str(bucketErr))

    def deleteBucket(self, name):
        """
        delete an existing bucket from the system
        :param name: the name of the bucket to be deleted
        """
        try:
            if self.checkExist(name):
                self.client.remove_bucket(name)
        except Exception as bucketErr:
            raise RuntimeError(str(bucketErr))

    def uploadFile(self, local, remote):
        """
        uploads a local file into an object storage bucket
        :param local: full path and filename of a file on the local file system
        :param remote: the destination path and filename in object storage
        """
        try:
            self.client.fput_object(self.bucket, remote, local)
        except Exception as fileErr:
            raise RuntimeError(str(fileErr))

    def downloadFile(self, remote, local):
        """
        downloads a remote object storage file into a local file
        :param remote: the source path and filename, including bucket in object storage
        :param local: the destination path and filename on the local file system
        """
        try:
            data = self.client.get_object(self.bucket, remote)
            with open(local, 'wb') as fileData:
                for d in data.stream(32*1024):
                    fileData.write(d)
        except Exception as fileErr:
            raise RuntimeError(str(fileErr))

    def streamFile(self, remote):
        """
        Pull the file as a stream from minio storage
        :param remote: the source path and filename, including bucket in google storage
        :return: string (stream)
        """
        try:
            data = self.client.get_object(self.bucket, remote)
            return data.read()
        except Exception as fileErr:
            raise RuntimeError(str(fileErr))

    def copyFile(self, sourceFile, destBucket='', destFile=''):
        """
        copies a file from one google storage location to another
        :param sourceFile: source path and filename (to be copied)
        :param destBucket: destination google storage bucket (could be the same)
        :param destFile: destination path and filename
        """
        if destBucket == '':
            destBucket = self.bucket
        if destFile == '':
            destFile = sourceFile

        try:
            copy_result = self.client.copy_object(destBucket, destFile, self.bucket + '/' + sourceFile)
        except Exception as fileErr:
            raise RuntimeError(str(fileErr))

    def deleteFile(self, fileName):
        """
        delete a file from a google storage bucket
        :param fileName: full path and filename to delete
        """
        if self.checkFileExists(fileName):
            try:
                self.client.remove_object(self.bucket, fileName)
            except Exception as fileErr:
                raise RuntimeError(str(fileErr))
        else:
            raise RuntimeError('StorageBase.deleteFile: Reqested File Not Found.')

    def getContents(self, folder=''):
        """
        return the contents of a bucket (or folder)
        :param folder:
        :return: list of files
        """
        fileList = []
        objects = self.client.list_objects_v2(self.bucket, prefix=folder)
        for obj in objects:
            fileList.append(obj.object_name.encode('utf-8'))

        return fileList

    def getFile(self, fileName):
        """
        get metadata on a given object out of storage
        :param fileName:
        :return: dictionary
        """
        meta = {}
        meta['name'] = fileName
        try:
            stat = self.client.stat_object(self.bucket, fileName)
            meta['size'] = stat.size
            meta['contentType'] = stat.content_type
            meta['lastModified'] = str(stat.last_modified)
            meta['etag'] = stat.etag
            meta['exists'] = True
        except:
            meta['exists'] = False
        return meta
