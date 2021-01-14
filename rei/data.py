import gspread
import os

class db:
    def __init__(self):
        location = os.path.dirname(os.path.realpath(__file__))
        self.gc = gspread.service_account(os.path.join(location,'config','gspread_creds.json'))
        self.wb = gc.open('properties')
