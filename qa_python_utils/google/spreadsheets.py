import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

from qa_python_utils import QuintoAndarLogger

logger = QuintoAndarLogger('GoogleSheetsClient')


class Spreadsheet(object):
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
            logger.error("m=get_dataframe_from_sheet, error={}".format(e.message))
            raise Exception(e.message)

        logger.info("m=get_dataframe_from_sheet, sheet found, returning DataFrame!")
        return pd.DataFrame(sheet.get_all_records())
