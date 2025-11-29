#------------------------------------------------------------
# Filename  : agent_sector_load_todos.py
# Project   : ava
#
# Descr     : This holds routine to load ims_load_todos with requests for
#             historic market data for tickers in a given sector that have
#             never been loaded before
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2019-10-30   1 MW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------



from datetime import datetime, timedelta, time
from infrastructure.blackboard.load_todos_manager import load_todos_manager
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db


def agent_sector_load_todos(database, rec_type, freq_type, inv_ticker, inv_sec_name, str_start_datetime, str_end_datetime, progress_status):

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
    
    # Bring up to specified datetime = chose yesterday at this time
    # starting 7 days ago at this time
    
    start_datetime     = datetime.combine(datetime.today() - timedelta(days=7), time.min) 
    end_datetime       = datetime.combine(datetime.today() - timedelta(days=1), time.min)   
    str_start_datetime = datetime.strftime(start_datetime,"%Y-%m-%d %H:%M:%S")
    str_end_datetime   = datetime.strftime(end_datetime,"%Y-%m-%d %H:%M:%S")
    progress_status    = ''
    
    agent_sector_load_todos(database           = database,
                            rec_type           = 'HISTMKTDATA_BID', 
                            freq_type          = '1 min',
                            inv_ticker         = '%', 
                            inv_sec_name       = 'Financial',
                            str_start_datetime = str_start_datetime, 
                            str_end_datetime   = str_end_datetime, 
                            progress_status    = progress_status)
    
    agent_sector_load_todos(database           = database,
                            rec_type           = 'HISTMKTDATA_ASK', 
                            freq_type          = '1 min',
                            inv_ticker         = '%', 
                            inv_sec_name       = 'Financial',
                            str_start_datetime = str_start_datetime, 
                            str_end_datetime   = str_end_datetime, 
                            progress_status    = progress_status)
    
    agent_sector_load_todos(database           = database,
                            rec_type           = 'HISTMKTDATA_TRADES', 
                            freq_type          = '1 min',
                            inv_ticker         = '%', 
                            inv_sec_name       = 'Financial',
                            str_start_datetime = str_start_datetime, 
                            str_end_datetime   = str_end_datetime, 
                            progress_status    = progress_status)
    
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database)  
    

