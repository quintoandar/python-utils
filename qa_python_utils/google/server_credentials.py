from __future__ import print_function

import json
import os

from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.appdata',
          'https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/drive.metadata',
          'https://www.googleapis.com/auth/drive.metadata.readonly',
          'https://www.googleapis.com/auth/drive.photos.readonly',
          'https://www.googleapis.com/auth/drive.readonly']
with open(os.getenv('QA_PYTHON_UTILS_CREDENTIALS_JSON', os.getenv('CREDENTIALS_JSON')),
          'r') as content_file:  # noqa
    CREDENTIALS_JSON = content_file.read()


def get_credentials():
    '''
    Authenticates a user, using a service account credential
    '''
    credentials = (ServiceAccountCredentials.
                   from_json_keyfile_dict(json.loads(CREDENTIALS_JSON),
                                          SCOPES))
    return credentials
