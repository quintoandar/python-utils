import logging
import os
import unicodedata

import httplib2shim
from apiclient import discovery
from apiclient.http import MediaFileUpload, MediaIoBaseUpload

import qa_python_utils.google.server_credentials as google_credentials

LOGGER = logging.getLogger(__name__)


class Drive(object):
    """
    Provides methos to access the google drive API
    """
    def __init__(self):
        self.authorize()
        self.FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'

    def authorize(self):
        """
        Initializes the google drive API, authenticating the user
        """
        self.credentials = google_credentials.get_credentials()
        self.http = self.credentials.authorize(httplib2shim.Http())
        self.service = discovery.build('drive', 'v3', http=self.http)

    def refresh(self):  # pragma: no cover
        self.credentials.refresh(self.http)

    def get_folders_by_name(self, name, folder_id=None):
        """
        Returns a list of google drive folders, identified by name or
        file_id
        """
        q = "mimeType = '%s' and name = '%s'" % (self.FOLDER_MIME_TYPE, name)
        if folder_id is not None:
            q += " and '" + folder_id + "' in parents"
        file_list = self.service.files().list(q=q).execute()
        return file_list['files']

    def list_files_single_page(self, folder_id=None, query=None):
        """
        Return a list of files from google drive. Passing a folder_id, returns
        the files for that folder.
        """
        if folder_id:
            query = ("'%s' in parents and mimeType != '%s'" %
                     (folder_id, self.FOLDER_MIME_TYPE))
        if query:
            file_list = self.service.files().list(q=query).execute()
            return file_list['files']

    def list_files(self, folder_id=None, query=None):
        """
        Return a list of files from google drive. Passing a folder_id, returns
        the files for that folder.
        """
        file_list = []
        if folder_id:
            query = ("'%s' in parents and mimeType != '%s'" %
                     (folder_id, self.FOLDER_MIME_TYPE))
        LOGGER.info('folder_id: {}'.format(folder_id))
        LOGGER.info('query: {}'.format(query))
        if query:
            result = self.service.files().list(q=query).execute()
            LOGGER.info('result size: {}'.format(len(result['files'])))
            file_list.extend(result['files'])
            while result.get('nextPageToken'):
                result = (self.service
                          .files()
                          .list(q=query, pageToken=result.get('nextPageToken'))
                          .execute())
                LOGGER.info('result size: {}'.format(len(result['files'])))
                file_list.extend(result['files'])
        LOGGER.info('file_list size: {}'.format(len(file_list)))
        return file_list

    def get_file_info(self, file_id):
        """
        Return the info for a file, containing id, name, description, etc
        """
        return self.service.files().get(fileId=file_id).execute()

    def get_file_media(self, file_id):
        """
        Get the file content, to download the file
        """
        return self.service.files().get_media(fileId=file_id).execute()

    def download_file(self, file_id, path):
        """
        Downloads a file from google drive
        """
        if not path or not file_id:
            return
        data = self.get_file_media(file_id)
        file_info = self.get_file_info(file_id)
        file_name = (unicodedata.normalize('NFD', file_info['name'])
                     .encode('ASCII', 'ignore')
                     .decode('utf-8'))
        download_file = open(os.path.join(path, file_name), 'wb')
        download_file.write(data)
        return file_name

    # pragma: no cover
    def upload_saved_file(self, file_path, description, parent_id):
        """
        Uploads a file saved in disk to google drive
        """
        file_name = os.path.basename(file_path)
        media_body = MediaFileUpload(file_path, resumable=True)
        return self.__upload_file(
            media_body,
            file_name,
            description,
            parent_id
        )

    def upload_in_memory_file(self, file_storage, description, parent_id):
        """
        Uploads a file saved in memory to google drive
        """
        file_name = os.path.basename(file_storage.filename)
        media_body = MediaIoBaseUpload(
            file_storage.stream,
            mimetype=file_storage.mimetype,
            resumable=True)
        return self.__upload_file(
            media_body,
            file_name,
            description,
            parent_id
        )

    def __upload_file(self, media_body, file_name, description, parent_id):
        body = {
            'title': file_name,
            'description': description,
            'name': file_name,
            'parents': [parent_id]
        }
        return (self.service
                .files()
                .create(body=body, media_body=media_body)
                .execute())

    def create_folder(self, folder_name, parent_id=None):
        """
        Creates a folder in google drive
        """
        body = {
            'mimeType': self.FOLDER_MIME_TYPE,
            'name': folder_name
        }
        if parent_id:
            body['parents'] = [parent_id]
        result = self.service.files().create(body=body).execute()
        return result['id']

    def delete_file(self, file_id):
        """
        Deletes a file or folder from google drive
        """
        result = self.service.files().delete(fileId=file_id).execute()
        return result

    def update_file(self, file_id, new_p, old_p):
        """
        Deletes a file or folder from google drive
        """
        result = self.service.files().update(fileId=file_id,
                                             addParents=new_p,
                                             removeParents=old_p).execute()
        return result

    def rename_file(self, file_id, new_parent, old_parent, new_name):
        """
        Deletes a file or folder from google drive
        """
        body = {'name': new_name}
        result = self.service.files().update(fileId=file_id,
                                             addParents=new_parent,
                                             removeParents=old_parent,
                                             body=body).execute()
        return result

    def grant_permission(self, folder_name, email):
        """
        Gives permission to a file or folder
        """
        folders = self.get_folders_by_name(folder_name)
        folder_id = folders[0]['id']
        body = {
            'emailAddress': email,
            'role': 'writer',
            'type': 'user'
        }
        result = self.service.permissions().create(fileId=folder_id,
                                                   body=body).execute()
        return result
