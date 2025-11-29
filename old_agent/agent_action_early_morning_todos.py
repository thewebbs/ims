#------------------------------------------------------------
# Filename  : agent_action_early_morning_todos.py
# Project   : ava
#
# Descr     : This holds routine to load a range of data into streak_summaries
#             and streak_details
#
# Params    : database
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-01-05   1 MW  Initial write
# ...
# 2021-12-19 100 DW  Added version
# 2021-12-26 101 MW  changed to use lto_start_datetime
#                    not lto_start_date throughout and
#                    changed ldo_date_loaded to 
#                    ldo_datetime_loaded
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


from datetime import datetime
from analysis.from_ILS import EarlyMorning.EarlyMorning  
from database.db_objects.ImsLoadDoneDB import ImsLoadDoneDB
from database.db_objects.ImsLoadTodoDB import delete_load_todo, get_load_todo, set_todo_status
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db


def agent_early_morning(database):
    
    print('=======================================================================')   
    print(' Start of early morning processing')
    print('=======================================================================')   

    # reset any WIP back to null to pick up again
    
    result = set_todo_status(database           = database,
                             lto_inv_ticker     = '%', 
                             lto_freq_type      = '%', 
                             lto_req_type       = 'EARLYAM', 
                             lto_start_datetime = '%', 
                             old_lto_status     = 'WIP', 
                             new_lto_status     = 'RDY'
                             ) 
            
    # go to the ims_load_todos table to find requests of type STREAK in priority and date order
    
    todo_list = get_load_todo(database           = database,
                              lto_inv_ticker     = '%', 
                              lto_freq_type      = '%', 
                              lto_req_type       = 'EARLYAM', 
                              lto_start_datetime = '%', 
                              lto_status         = 'RDY',
                              limit_one          = False
                              )
        
    for hmd_inv_ticker, hmd_freq_type, hmd_req_type, hmd_start_date, hmd_end_date in todo_list:
        
        # set this load_todos record to WIP
        # record this request is now in progress
        
        result = set_todo_status(database           = database,
                                 lto_inv_ticker     = hmd_inv_ticker, 
                                 lto_freq_type      = hmd_freq_type, 
                                 lto_req_type       = hmd_req_type, 
                                 lto_start_datetime = hmd_start_date, 
                                 old_lto_status     = '%', 
                                 new_lto_status     = 'WIP'
                                 ) 
        
        # Note these are the dates that the load to do was created - in the code we will
        # find the latest date for which we have prices for that ticker and use that date
        # it will be change in newEarlyMorning.collect_early_morning_stats()
        
        trading_date          = hmd_start_date
        previous_trading_date = hmd_start_date  
                   
        newEarlyMorning = EarlyMorning(database              = database,
                                       analysis_type         = 'EARLYAM', 
                                       inv_ticker            = hmd_inv_ticker, 
                                       trading_date          = trading_date,
                                       previous_trading_date = previous_trading_date,
                                       closing_bid           = 0, 
                                       closing_ask           = 0, 
                                       opening_bid           = 0, 
                                       opening_ask           = 0, 
                                       overnight_change_bid  = 0, 
                                       overnight_change_ask  = 0,
                                       highest_bid           = 0, 
                                       highest_ask           = 0, 
                                       lowest_bid            = 0, 
                                       lowest_ask            = 0, 
                                       morning_change_bid    = 0, 
                                       morning_change_ask    = 0)
    
        newEarlyMorning.collect_early_morning_stats()
             
        # create the load_done record
    
        date_now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
         
        load_done_data = ImsLoadDoneDB(database            = database,
                                       ldo_inv_ticker      = hmd_inv_ticker,
                                       ldo_freq_type       = hmd_freq_type,
                                       ldo_req_type        = hmd_req_type,
                                       ldo_start_datetime  = hmd_start_date,
                                       ldo_end_datetime    = hmd_end_date,
                                       ldo_datetime_loaded = date_now
                                       )
        
        result = load_done_data.select_DB()
        
        if result == None:
            load_done_data.insert_DB()
        else:
            load_done_data.update_DB()
               
        # delete the load_todos record
        delete_load_todo(database           = database,
                         lto_inv_ticker     = hmd_inv_ticker, 
                         lto_freq_type      = hmd_freq_type, 
                         lto_req_type       = hmd_req_type, 
                         lto_start_datetime = hmd_start_date
                         )
        
    print(' ')
    print('=======================================================================')   
    print(' End of early morning processing')
    print('=======================================================================')   
    
    return


if __name__ == "__main__":

    print("Open db")
    print(" ")
    
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
    
    agent_early_morning(database = database)
       
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database)  
    

