import os
from google.drive import Drive
from apiclient.discovery import build
from apiclient.http import HttpMockSequence
import json


API_KEY = 'AIzaSyB-YmrKpooXwycp_F81yd0yHEMWs9AAxRA'
FOLDERS_DISCOVERY = json.dumps({'files': [{'id': '1', 'name': 'test.pdf'}]})
FOLDERS_DISCOVERY_PAGED = json.dumps({
    'files': [{'id': '2', 'name': 'test2.pdf'}],
    'nextPageToken': 'a'})
FILE_INFO_DISCOVERY = json.dumps({'name': 'test.pdf'})
FILE_MEDIA_DISCOVERY = json.dumps({'media': 'a'})
CREATE_FOLDER_DISCOVERY = json.dumps({'id': '2'})
GENERIC_DISCOVERY = json.dumps({'id': '2'})
PATH = os.getenv('PYTHONPATH', '')


def test_get_folders():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FOLDERS_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    file_list = drive.get_folders_by_name('test')
    assert len(file_list) == 1
    assert file_list[0]['name'] == 'test.pdf'


def test_get_folders_2():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FOLDERS_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    file_list = drive.get_folders_by_name('test', '1')
    assert len(file_list) == 1
    assert file_list[0]['name'] == 'test.pdf'


def test_list_files_sp_1():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FOLDERS_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    file_list = drive.list_files_single_page(query='test')
    assert len(file_list) == 1
    assert file_list[0]['name'] == 'test.pdf'


def test_list_files_sp_2():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FOLDERS_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    file_list = drive.list_files_single_page(query='test', folder_id='1')
    assert len(file_list) == 1
    assert file_list[0]['name'] == 'test.pdf'


def test_list_files_1():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FOLDERS_DISCOVERY_PAGED),
                             ({'status': '200'}, FOLDERS_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    file_list = drive.list_files(query='test')
    assert len(file_list) == 2
    assert file_list[0]['name'] == 'test2.pdf'


def test_list_files_2():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FOLDERS_DISCOVERY_PAGED),
                             ({'STATUS': '200'}, FOLDERS_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    file_list = drive.list_files(query='test', folder_id='1')
    assert len(file_list) == 2
    assert file_list[0]['name'] == 'test2.pdf'


def test_get_file_info():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FILE_INFO_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    file_info = drive.get_file_info('1')
    assert file_info['name'] == 'test.pdf'


def test_get_file_media():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FILE_MEDIA_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    file_media = drive.get_file_media('1')
    assert file_media == b'{"media": "a"}'


def test_download_file_empty():
    drive = Drive()
    assert drive.download_file(None, '1') is None
    assert drive.download_file(None, None) is None
    assert drive.download_file('1', None) is None


def test_download_file(datadir):
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FILE_MEDIA_DISCOVERY),
                             ({'status': '200'}, FILE_INFO_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    drive.download_file('1', str(datadir.join('.')))
    with open(str(datadir.join('test.pdf')), 'rb') as pdf_file:
        assert [b'{"media": "a"}'] == pdf_file.readlines()


def test_create_folder():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, CREATE_FOLDER_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    folder_id = drive.create_folder('folder_name')
    assert folder_id == '2'


def test_create_folder_with_parent():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, CREATE_FOLDER_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    folder_id = drive.create_folder('folder_name', parent_id='1')
    assert folder_id == '2'


def test_delete_file():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, GENERIC_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    result = drive.delete_file('1')
    assert result == b'{"id": "2"}'


def test_update_file():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, GENERIC_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    result = drive.update_file('1', '2', '3')
    assert result == {"id": "2"}


def test_rename_file():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, GENERIC_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    result = drive.rename_file('1', '2', '3', 'new_name')
    assert result == {"id": "2"}


def test_grant_permission():
    drive = Drive()
    http = HttpMockSequence([({'status': '200'}, FOLDERS_DISCOVERY),
                             ({'status': '200'}, GENERIC_DISCOVERY)])
    service = build('drive', 'v3', http=http, developerKey=API_KEY)
    drive.service = service
    result = drive.grant_permission('folder_name',
                                    'leonardo.almeida@quintoandar.com.br')
    assert result == {"id": "2"}
