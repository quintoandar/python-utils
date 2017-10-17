from __future__ import print_function

import json
import os
import logging

log = logging.getLogger(__name__)

from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.appdata',
          'https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/drive.metadata',
          'https://www.googleapis.com/auth/drive.metadata.readonly',
          'https://www.googleapis.com/auth/drive.photos.readonly',
          'https://www.googleapis.com/auth/drive.readonly']

CREDENTIALS_JSON = None
try:
    with open(os.getenv('QA_PYTHON_UTILS_CREDENTIALS_JSON', os.getenv('CREDENTIALS_JSON')),
              'r') as content_file:  # noqa
        CREDENTIALS_JSON = content_file.read()
except Exception:
    log.info("Problem reading QA_PYTHON_UTILS_CREDENTIALS_JSON from file using content as is.")
    if CREDENTIALS_JSON is None:
        CREDENTIALS_JSON = os.getenv('QA_PYTHON_UTILS_CREDENTIALS_JSON', os.getenv('CREDENTIALS_JSON'))

log.info("Using CREDENTIALS_JSON: {}".format(CREDENTIALS_JSON))


def get_credentials():
    '''
    Authenticates a user, using a service account credential
    '''
    credentials = (ServiceAccountCredentials.
                   from_json_keyfile_dict(json.loads(CREDENTIALS_JSON),
                                          SCOPES))
    return credentials
