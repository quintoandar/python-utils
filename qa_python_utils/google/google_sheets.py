import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

from qa_python_utils import QuintoAndarLogger

logger = QuintoAndarLogger('GoogleSheetsClient')


class GoogleSheetsClient(object):
    """
    This client needs a credentials json called Service Account from Google Cloud Platform
    You have to create that in your project, and share sheet with Service Account e-mail
    And pass as arguments to method a sheet Name and sheet Id
    """
    def __init__(self, credentials):
        self.scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials, self.scope)
        self.gsheets = gspread.authorize(self.credentials)

    @logger
    def get_dataframe_from_sheet(self, sheet_name, sheet_id):
        working_sheet = self.gsheets.open_by_key(sheet_id)

        try:
            logger.info("m=get_dataframe_from_sheet, name={}, id={}".format(sheet_name, sheet_id))
            sheet = working_sheet.worksheet(sheet_name)
        except Exception as e:
            raise Exception("m=get_dataframe_from_sheet, error={}".format(e.message))

        logger.info("m=get_dataframe_from_sheet, sheet found, returning DataFrame!")
        return pd.DataFrame(sheet.get_all_records())
