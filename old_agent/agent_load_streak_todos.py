#------------------------------------------------------------
# Filename  : agent_load_streak_todos.py
# Project   : ava
#
# Descr     : This holds routine to load ims_load_todos with requests for streak detection
#
# Params    : database
#             rec_type
#             freq_type
#             inv_tivker
#             inv_sec_name
#             str_start_datetime
#             str_end_datetime
#             progress_status
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2019-11-17   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from datetime import datetime, timedelta, time
from infrastructure.blackboard.load_todos_manager import load_todos_manager
from utils.config import DEBUG, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD, REPORT_FOLDER_OSX
from utils.utils_database import close_db, open_db
from utils.utils_dates import daterange


def agent_load_streak_todos(database, rec_type, freq_type, inv_ticker, inv_sec_name,
                            str_start_datetime, str_end_datetime, progress_status):

    load_todos_manager(database           = database,
                       rec_type           = rec_type, 
                       freq_type          = freq_type, 
                       inv_ticker         = inv_ticker, 
                       inv_sec_name       = inv_sec_name,
                       str_start_datetime = str_start_datetime, 
                       str_end_datetime   = str_end_datetime, 
                       progress_status    = progress_status)

    return


if __name__ == "__main__":

    print("Open db")
    print(" ")

    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
    
    # We want to load todos up to end of business yesterday. If it runs on a 
    # monday it will just get no data for sat and sun
      
    rec_type           = 'STREAK'
    freq_type          = 'NA'
    inv_ticker         = '%' 
    inv_sec_name       = '%'
    str_start_datetime = '%' 
    str_end_date       = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    str_end_datetime   = str_end_date + " 23:59:59"
    progress_status    = 'RDY'
    
    agent_load_streak_todos(database           = database, 
                            rec_type           = rec_type, 
                            freq_type          = freq_type,
                            inv_ticker         = inv_ticker, 
                            inv_sec_name       = inv_sec_name,
                            str_start_datetime = str_start_datetime, 
                            str_end_datetime   = str_end_datetime, 
                            progress_status    = progress_status)  
    
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database)  
    

