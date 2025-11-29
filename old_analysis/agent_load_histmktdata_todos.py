#------------------------------------------------------------
# Filename  : agent_load_histmktdata_todos.py
# Project   : ava-trade
#
# Descr     : This holds routine to load ims_load_todos with requests for
#             historic market data
#
# Params    : database
#             rec_type
#             freq_type
#             inv_ticker
#             inv_sec_name
#             str_start_datetime
#             str_end_datetime
#             progress_status
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2019_09-19   1 MW  initial write
# ...
# 2021-07-14     DW  moved to ILS-ava
# 2021-08-25 100 DW  added version 
# 2022-11-05 200 DW  reorg
# 2022-11-18 201 DW  reworked for ava
# 2022-12-12 202 DW  moved to ava-trade
# 2023-01-03 205 DW  added delimiting comments
#------------------------------------------------------------

from datetime                                     import datetime, timedelta
from utils.config                                 import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database                         import close_db, open_db
from infrastructure.blackboard.load_todos_manager import load_todos_manager

#============================================================================================================================
# methods
#============================================================================================================================

#----------------------------------------------------------------------------------------------------------------------------
# 
#----------------------------------------------------------------------------------------------------------------------------

def agent_load_histmktdata_todos(database, rec_type, freq_type, inv_ticker, inv_sec_name,
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


#============================================================================================================================
# main
#============================================================================================================================

if __name__ == "__main__":

    print("-----------------------------")
    print("agent_load_histmktdata_todos - starting")
    print(" ")
        
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)   

    #
    # We want to load todos up to end of business yesterday. If it runs on a 
    # monday it will just get no data for sat and sun
    #
      
    freq_type          = '1 min' 
    inv_sec_name       = '%'
    str_start_datetime = '%' 
    str_end_date       = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    str_end_datetime   = str_end_date + " 23:59:59"
    progress_status    = 'RDY'
    rec_type           = 'HISTMKTDATA_BID'
    inv_ticker         = '%'

    agent_load_histmktdata_todos(database           = database, 
                                 rec_type           = rec_type, 
                                 freq_type          = freq_type,
                                 inv_sec_name       = inv_sec_name,
                                 inv_ticker         = inv_ticker, 
                                 str_start_datetime = str_start_datetime, 
                                 str_end_datetime   = str_end_datetime, 
                                 progress_status    = progress_status) 
    
    rec_type = 'HISTMKTDATA_ASK'

    agent_load_histmktdata_todos(database           = database, 
                                 rec_type           = rec_type, 
                                 freq_type          = freq_type,
                                 inv_sec_name       = inv_sec_name,
                                 inv_ticker         = inv_ticker, 
                                 str_start_datetime = str_start_datetime, 
                                 str_end_datetime   = str_end_datetime, 
                                 progress_status    = progress_status) 
    
    '''
    DW 12-dec-22 not getting trades for now
    
    rec_type = 'HISTMKTDATA_TRADES'
    
    agent_load_histmktdata_todos(database           = database, 
                                 rec_type           = rec_type, 
                                 freq_type          = freq_type,
                                 inv_sec_name       = inv_sec_name,
                                 inv_ticker         = inv_ticker, 
                                 str_start_datetime = str_start_datetime, 
                                 str_end_datetime   = str_end_datetime, 
                                 progress_status    = progress_status) 
    
    '''
    
    close_db(database = database)  
    
    print("-----------------------------")
    print("agent_load_histmktdata_todos - finished")
    print("-----------------------------")

