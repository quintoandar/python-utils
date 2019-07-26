import gspread
import pandas as pd
import xlsxwriter
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
        self.scope = scope
        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials, scope)
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

    @logger
    def clear_data_from_sheet(self, sheet_name, sheet_id, remove_header=False):
        if 'readonly' in self.scope:
            raise PermissionError(
                "m=clear_data_from_sheet, scope={}, msg=Provided scope does not have sufficient access level".format(
                    self.scope))
        try:
            working_sheet = self.gsheets.open_by_key(sheet_id)
            sheet = working_sheet.worksheet(sheet_name)
        except Exception as e:
            raise FileNotFoundError("m=clear_data_from_sheet, error={}".format(e.message))

        logger.info("m=clear_data_from_sheet, msg=Sheet found")
        try:
            if remove_header:
                logger.info("m=clear_data_from_sheet, sheet={}, msg=Clearing entire sheet...".format(sheet_name))
                sheet.clear()
            else:
                end_column = xlsxwriter.utility.xl_col_to_name(sheet.col_count - 1)
                target = "'{0}'!A2:{1}{2}".format(sheet_name, end_column, str(sheet.row_count))
                logger.info("m=clear_data_from_sheet, target_cells={}, msg=Clearing data...".format(target))
                working_sheet.values_clear(target)
        except Exception as e:
            raise Exception("m=clear_data_from_sheet, error={}".format(e.message))

        logger.info("m=clear_data_from_sheet, msg=Data cleared...")
