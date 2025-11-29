#------------------------------------------------------------
# Filename  : agent_load_earning_todos.py
# Project   : ava
#
# Descr     : This holds routine to load ims_load_todos with requests for
#             earnings
#
# Params    : database
#             rec_type
#             inv_ticker
#             str_start_datetime
#             str_enddatetime
#             progress_status
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-01-24   1 MW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


from datetime import datetime
from infrastructure.blackboard.load_todos_manager import load_todos_manager
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db


def agent_load_earning_todos(database, rec_type, inv_ticker, 
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
        
    # note that the date time isn't used for DIV as it picks up the latest
    # date for which there are prices but we have to have one here - using
    # today's date so that we can see in ims_load_todos how long a record has
    # been sitting there 

    rec_type           = 'EARN'
    freq_type          = 'None'
    inv_sec_name       = '%'
    str_start_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    str_end_datetime   = str_start_datetime
    progress_status    = 'RDY'
    inv_ticker         = '%' 

    agent_load_earning_todos(database           = database, 
                             rec_type           = rec_type, 
                             inv_ticker         = inv_ticker, 
                             str_start_datetime = str_start_datetime, 
                             str_end_datetime   = str_end_datetime, 
                             progress_status    = progress_status)    
    
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database)  
    

