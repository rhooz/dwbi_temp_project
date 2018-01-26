from oauth2client import client
from collections import defaultdict
from google.cloud import storage
import google.oauth2.credentials
import yaml
import sys
from StorageBase import StorageBase

class GSHelper(StorageBase):
    """some helper functions for interacting with Google Cloud Platform"
       derived from: https://cloud.google.com/storage/doc/xml-api/gspythonlibrary"""

    def _storeConfig(self):
        try:
            self.agent = self.config['agent']
        except:
            self.agent = ''

        if self.agent == '':
            self.client = storage.Client(project=self.projectId)
        else:
            self._getCredentials()
            self.client = storage.Client(project=self.projectId, credentials=self.credentials)
        self.blob = storage.Blob

    def _authUserCredentials(self):
        """run a human through a flow to get a token to support API calls
           note: started from google dbm examples"""
        scope = 'https://www.googleapis.com/auth/devstorage.full_control https://www.googleapis.com/auth/cloud-platform'
        flow = client.OAuth2WebServerFlow(
            client_id=self.config['client-id'], client_secret=self.config['client-secret'],
            scope=scope, user_agent=self.agent,
            redirect_uri='urn:ietf:wg:oauth:2.0:oob')

        authorize_url = flow.step1_get_authorize_url()

        print ('Log into the Google Account you use to access your DBM account'
               'and go to the following URL: \n%s\n' % (authorize_url))
        print 'After approving the token enter the verification code (if specified).'
        code = raw_input('Code: ').strip()

        try:
            self.credentials = flow.step2_exchange(code)
        except client.FlowExchangeError, e:
            print 'Authentication has failed: %s' % e
            sys.exit(1)

    def _saveCredentials(self):
        """save the current set of credentials to a local yaml file
           note: startec from google dbm examples"""
        with open(self.config['lib-directory'] + '/' + self.agent + '.yaml', 'wb') as handle:
            handle.write(yaml.dump({
                'client_id': self.credentials.client_id,
                'client_secret': self.credentials.client_secret,
                'refresh_token': self.credentials.refresh_token}))

    def _retrieveCredentials(self):
        """retrieve the last set of credentials from local yaml file
           note: started from google dbm examples"""
        with open(self.config['lib-directory'] + '/' + self.agent + '.yaml', 'rb') as handle:
            auth_data = yaml.load(handle.read())
            return auth_data

    def _getCredentials(self):
        """pull the set of credentials for running the API calls
           note: started from google dbm examples"""
        try:
            auth_data = self._retrieveCredentials()
            self.credentials = google.oauth2.credentials.Credentials(
                'access_token',
                client_id=auth_data['client_id'],
                client_secret=auth_data['client_secret'],
                refresh_token=auth_data['refresh_token'],
                token_uri='https://accounts.google.com/o/oauth2/token')
        except IOError:
            self._authUserCredentials()
            self._saveCredentials()

    def createBucket(self, name):
        """
        creates a new google storage bucket
        :param name: the name of the new bucket to be created
        """
        try:
            self.client.create_bucket(name)
        except Exception as gsErr:
            raise RuntimeError(str(gsErr))

    def uploadFile(self, local, remote):
        """
        uploads a local file into a google storage bucket
        :param local: full path and filename of a file on the local file system
        :param remote: the destination path and filename in google storage
        """
        buck = self.client.get_bucket(self.bucket)
        # blob of the destination file is created
        blob = self.blob(remote, buck)
        # local file is read in into the blob to upload as objName
        blob.upload_from_filename(local)

    def downloadFile(self, remote, local):
        """
        downloads a remote google storage file into a local file
        :param remote: the source path and filename, including bucket in google storage
        :param local: the destination path and filename on the local file system
        """
        # Modified from using boto, Function calls are retained to protect downstream calls
        # bucket is split from remote format [bucket]/[folder]/[file]
        bucketName = remote.split("/")[0]
        # second part of remote is used to locate the object within the bucket
        objName = "/".join(x for x in remote.split("/")[1:])
        buck = self.client.get_bucket(bucketName)
        # blob of the source file is read in
        blob = self.blob(objName, buck)
        # blob is downloaded to a fileName in local
        blob.download_to_filename(local)

    def streamFile(self, remote):
        """
        Pull the file as a stream from google storage
        :param remote: the source path and filename, including bucket in google storage
        :return: string (stream)
        """
        # source e.g. [bucket]/[directory]/[filename]"
        bucketName = remote.split("/")[0]
        objName = "/".join(x for x in remote.split("/")[1:])
        buck = self.client.get_bucket(bucketName)
        # source file is read into a blob inside the bucket
        blob = self.blob(objName, buck)
        # grab the file and stream into the holder object
        fileCont=blob.download_as_string()
        # stream the contents out to the caller as string
        return fileCont

    def copyFile(self, sourceFile, destBucket='', destFile=''):
        """
        copies a file from one google storage location to another
        :param sourceBucket: source google storage bucket
        :param sourceFile: source path and filename (to be copied)
        :param destBucket: destination google storage bucket (could be the same)
        :param destFile: destination path and filename
        """
        buck = self.client.get_bucket(self.bucket)
        srcBlob = self.blob(sourceFile, buck)
        dest = self.client.get_bucket(destBucket)
        buck.copy_blob(srcBlob, dest, new_name=destFile)

    def deleteFile(self, fileName):
        """
        delete a file from a google storage bucket
        :param bucket: the google storage bucket within which to act
        :param fileName: full path and filename to delete
        """
        try:
            buck = self.client.get_bucket(self.bucket)
            buck.delete_blob(fileName)
        except Exception as deleteErr:
            if 'Not Found' in str(deleteErr):
                raise StandardError("GSHelper.deleteFile: Not Found. Details: " + str(deleteErr))
            else:
                raise StandardError("GSHelper.deleteFile: Error in deleting the requested file. Details: " + str(deleteErr))

    def getFile(self, fileName):
        """
        get a given object out of storage
        :param fileName:
        :return:
        """
        meta = {}
        meta['name'] = fileName
        try:
            buck = self.client.get_bucket(self.bucket)
            blob = self.blob(fileName, buck)
            if blob.exists():
                meta['size'] = blob.size
                meta['contentType'] = blob.content_type
                meta['lastModified'] = str(blob.time_created)
                meta['etag'] = blob.etag
                meta['exists'] = True
            else:
                meta['exists'] = False
        except:
            meta['exists'] = False
        return meta

    def getContents(self, folder=''):
        """
        return the contents of a bucket (or folder)
        :param folder:
        :return: list of files
        """
        buck = self.client.get_bucket(self.bucket)
        fileList = []
        for obj in buck.list_blobs():
            if folder != '':
                if folder in obj.name:
                    fileList.append(obj.name)
            else:
                fileList.append(obj.name)

        return fileList
