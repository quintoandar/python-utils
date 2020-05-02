import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from qa_python_utils import QuintoAndarLogger

logger = QuintoAndarLogger('GoogleSheetsClient')


class GoogleSheetsClient(object):
    """
    This client needs a credentials json called Service Account from Google Cloud Platform
    You have to create that in your project, and share sheet with Service Account e-mail
    Also you need a scope URL from google sheets to use as class argument
    And pass as arguments to method a sheet Name and sheet Id
    """
    def __init__(self, credentials, scope):
        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials, scope)
        self.gsheets = gspread.authorize(self.credentials)

    @logger
    def get_dataframe_from_sheet(self, sheet_name, sheet_id):

        try:
            working_sheet = self.gsheets.open_by_key(sheet_id)
            logger.info("m=get_dataframe_from_sheet, name={}, id={}".format(sheet_name, sheet_id))
            sheet = working_sheet.worksheet(sheet_name)

        except gspread.exceptions.SpreadsheetNotFound as e:
            raise Exception("m=get_dataframe_from_sheet, error=Spreadsheet was not found - {}".format(str(e)))

        except gspread.exceptions.WorksheetNotFound as e:
            raise Exception("m=get_dataframe_from_sheet, error=Worksheet was not found - {}".format(str(e)))

        except Exception as e:
            raise Exception("m=get_dataframe_from_sheet, error={}".format(str(e)))

        logger.info("m=get_dataframe_from_sheet, sheet found, returning DataFrame!")
        return pd.DataFrame(sheet.get_all_records())