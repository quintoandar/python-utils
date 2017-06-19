from __future__ import print_function
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.appdata',
          'https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/drive.metadata',
          'https://www.googleapis.com/auth/drive.metadata.readonly',
          'https://www.googleapis.com/auth/drive.photos.readonly',
          'https://www.googleapis.com/auth/drive.readonly']

print("aaa = " + os.getenv('QA_PYTHON_UTILS_CREDENTIALS_JSON', os.getenv('CREDENTIALS_JSON')))
CREDENTIALS_JSON = os.getenv('QA_PYTHON_UTILS_CREDENTIALS_JSON', os.getenv('CREDENTIALS_JSON'))  # noqa


def get_credentials():
    '''
    Authenticates a user, using a service account credential
    '''
    credentials = (ServiceAccountCredentials.
                   from_json_keyfile_dict(json.loads(CREDENTIALS_JSON),
                                          SCOPES))
    return credentials
