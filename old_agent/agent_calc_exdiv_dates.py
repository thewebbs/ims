#------------------------------------------------------------
# Filename  : agent_calc_exdiv_dates.py
# Project   : ava
#
# Descr     : This tries to guess what the exdiv dates will be for future period
#             based on the fundamental data recieved for past divs
#
# Params    : database
#             inv_ticker
#             str_start_date
#             str_end_date
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2022-01-05 100 DW  Initial write
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD

from datetime import datetime
from utils.utils_database import close_db, open_db


def agent_calc_exdiv_dates(database, inv_ticker, str_start_datetime, str_end_datetime):

    #
    # We need to guess the exdiv date based on historic loaded fundamental div data
    # We do this by finding the 

    return


if __name__ == "__main__":

    print("Open db")
    print(" ")
    print("host      : ", DB_HOST)
    print("user_name : ", DB_USER_NAME)
    
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
    
    print("Connected to DB")
    
    inv_ticker     = '%' 
    str_start_date = '2022-01-01'
    str_end_date   = '2022-12-31'
    
    agent_calc_exdiv_dates(database       = database, 
                           inv_ticker     = inv_ticker, 
                           str_start_date = str_start_date, 
                           str_end_date   = str_end_date)

    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database)  
    
    print("Closed db")
    print(" ")
