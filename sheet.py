import gspread
from gspread.utils import a1_to_rowcol


GOOGLE_API_AUTH = None
GOOGLE_API_SPREADSHEET_ID = None

from local_settings import *

_service_spreadsheets = None
_spreadsheet = None


def get_service_spreadsheets():
    global _service_spreadsheets
    if not _service_spreadsheets:
        _service_spreadsheets = gspread.service_account_from_dict(GOOGLE_API_AUTH)
    return _service_spreadsheets


def get_sheet():
    global _spreadsheet
    if not _spreadsheet:
        _spreadsheet = get_service_spreadsheets().open_by_key(GOOGLE_API_SPREADSHEET_ID)
    return _spreadsheet


class SheetValues:
    def __init__(self, worksheet: gspread.Worksheet):
        self.data = worksheet.get_all_values() if isinstance(worksheet, gspread.Worksheet) else worksheet

    def __getitem__(self, key):
        if isinstance(key, str) and ':' in key:
            key = slice(*key.split(':', 1))
        if isinstance(key, slice):
            row_start, col_start = a1_to_rowcol(key.start)
            row_end, col_end = a1_to_rowcol(key.stop)
            if row_end == row_start:
                return self.data[row_start-1][col_start-1:col_end]
            elif col_start == col_end:
                return [row[col_start - 1] for row in self.data[row_start - 1:row_end]]
            else:
                return [row[col_start - 1:col_end] for row in self.data[row_start - 1:row_end]]
        else:
            row, col = a1_to_rowcol(key)
            return self.data[row - 1][col - 1]
