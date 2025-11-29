#------------------------------------------------------------
# Filename  : agent_action_streak_todos.py
# Project   : ava
#
# Descr     : This holds routine to load a range of data into streak_summaries and streak_details
#
# Params    : None
#
# History  :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2019-08-07   1 MW  Initial write
# ...
# 2021-08-27 100 DW  Added version and moved to ILS-ava 
# 2021-08-31 101 DW  Changed start_date -> start_datetime and end_date -> end_datetime
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------
  
from analysis.from_ILS import StreakProcessing.StreakProcessing
from database.db_objects.ImsHistMktDataDB import get_ticker_with_price_in_date_range
from database.db_objects.ImsLoadDoneDB import ImsLoadDoneDB
from database.db_objects.ImsLoadTodoDB import delete_load_todo, get_load_todo, set_todo_status
from datetime import datetime, timedelta, date
from utils.config import DEBUG, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD, REPORT_FOLDER_OSX
from utils.utils_database import close_db, open_db
from utils.utils_dates import daterange


def agent_action_streak_todos(database):
    
    #
    # reset any WIP back to RDY to pick up again
    #
    
    result = set_todo_status(database           = database,
                             lto_inv_ticker     = '%', 
                             lto_freq_type      = '%', 
                             lto_req_type       = 'STREAK', 
                             lto_start_datetime = '%', 
                             old_lto_status     = 'WIP', 
                             new_lto_status     = 'RDY'
                             ) 
            
    #
    # go to the ims_load_todos table to find requests of type STREAK in priority and date order
    #
    
    todo_list = get_load_todo(database           = database,
                              lto_inv_ticker     = '%', 
                              lto_freq_type      = '%', 
                              lto_req_type       = 'STREAK', 
                              lto_start_datetime = '%', 
                              lto_status         = 'RDY',
                              limit_one          = False
                              )

    process_streak_todo_list(database          = database, 
                             todo_list         = todo_list,
                             produce_graph     = False, 
                             display_to_screen = False )    
    
    return 


def process_streak_todo_list(database, todo_list, produce_graph, display_to_screen):
        
    for hmd_inv_ticker, hmd_freq_type, hmd_req_type, hmd_start_date, hmd_end_date in todo_list:
        
        #
        # set this load_todos record to WIP
        # record this request is now in progress
        #
        
        result = set_todo_status(database           = database,
                                 lto_inv_ticker     = hmd_inv_ticker, 
                                 lto_freq_type      = hmd_freq_type, 
                                 lto_req_type       = hmd_req_type, 
                                 lto_start_datetime = hmd_start_date, 
                                 old_lto_status     = '%', 
                                 new_lto_status     = 'WIP'
                                 ) 

        
        #
        # process the streaks for this record
        #
        
        total_profit   = 0
        this_freq_type = '1 min'
    
        for single_date in daterange(hmd_start_date, hmd_end_date):
            
            start_date_as_string = single_date.strftime("%Y-%m-%d") + " 00:00:00"
            end_date_as_string   = single_date.strftime("%Y-%m-%d") + " 23:59:59"
                       
            newStreakProcessing  = StreakProcessing(database          = database,
                                                    inv_ticker        = hmd_inv_ticker, 
                                                    start_date        = start_date_as_string, 
                                                    end_date          = end_date_as_string, 
                                                    produce_graph     = produce_graph, 
                                                    display_to_screen = display_to_screen, 
                                                    output_path_name  = REPORT_FOLDER_OSX, 
                                                    number_ups        = 0, 
                                                    number_downs      = 0, 
                                                    number_flats      = 0, 
                                                    profit            = 0, 
                                                    freq_type         = this_freq_type
                                                    )
    
            newStreakProcessing.detectstreaks()
        
            total_profit += newStreakProcessing.profit
            
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
        
        #load_done_data.write_DB()
        
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
        final_results = "END OF RUN FOR %s - TOTAL PROFIT $%2.2f" % (hmd_inv_ticker, total_profit)
        print(final_results)
        print('=======================================================================')   
        
        total_profit = 0
    
    return


if __name__ == "__main__":

    print("Open db")
    print(" ")
 
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
       
    agent_action_streak_todos(database = database)
       
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database) 
    

