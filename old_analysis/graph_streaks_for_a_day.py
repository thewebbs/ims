# Filename  : graph_streaks_for_a_day.py
# Project   : ava
#
# Descr     : This holds routine to graph the streaks calculated for a day 
#             - does by creating the load todo and then running it again
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-08-04   1 MW  Initial write
# ...
# 2021-09-05 100 DW  Added version and moved to ILS-ava
# 2021-12-27 101 MW  Changed start_date to start_datetime for
#                    both get_load_todo and ImsLoadTodoDB
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from ava_agents.agent_action_streak_todos import process_streak_todo_list
from database.db_objects.ImsLoadTodoDB import get_load_todo, ImsLoadTodoDB
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db


def graph_streaks_for_a_day(database, inv_ticker, inv_sec_name, freq_type, req_type, start_datetime, end_datetime, req_status):
       
    new_ims_load_todo = ImsLoadTodoDB(database           = database,
                                      lto_inv_ticker     = inv_ticker,
                                      lto_freq_type      = freq_type,
                                      lto_req_type       = req_type, 
                                      lto_start_datetime = start_datetime, 
                                      lto_end_date       = end_datetime, 
                                      lto_status         = req_status, 
                                      lto_priority       = 1)
    new_ims_load_todo.insert_DB()

    todo_list = get_load_todo(database           = database,
                              lto_inv_ticker     = inv_ticker, 
                              lto_freq_type      = freq_type, 
                              lto_req_type       = req_type, 
                              lto_start_datetime = '%', 
                              lto_status         = req_status,
                              limit_one          = False
                              )
    print(todo_list)
            
    # now action this load_todo record
    process_streak_todo_list(database = database,
                             todo_list = todo_list,
                             produce_graph     = True, 
                             display_to_screen = True)
        
    return


if __name__ == "__main__":

    print("Open db")
    print(" ")
 
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)

    #start_datetime_list = ['2020-07-27 00:00:00', '2020-07-28 00:00:00', '2020-07-29 00:00:00','2020-07-30 00:00:00']    
    #end_datetime_list   = ['2020-07-28 00:00:00', '2020-07-29 00:00:00', '2020-07-30 00:00:00','2020-08-01 00:00:00']    
    #ticker_list = ['CM.TO','BMO.TO','ENB.TO','LB.TO']
    
    start_datetime_list = ['2020-05-27 00:00:00','2020-05-28 00:00:00','2020-05-29 00:00:00']    
    end_datetime_list   = ['2020-05-28 00:00:00','2020-05-29 00:00:00','2020-05-30 00:00:00']  
    ticker_list = ['CM.TO','CM.NYSE']
    
    for this_date in range(0,len(start_datetime_list)):
        this_start_datetime = start_datetime_list[this_date] 
        this_end_datetime   = end_datetime_list[this_date]
        print(this_start_datetime,this_end_datetime )
        
        for this_ticker in ticker_list:
            print(this_ticker)
            
            graph_streaks_for_a_day(database = database,
                                    inv_ticker = this_ticker, 
                                    inv_sec_name = '%',
                                    freq_type = '1 min', 
                                    req_type = 'STREAK', 
                                    start_datetime = this_start_datetime,
                                    end_datetime = this_end_datetime,
                                    req_status = 'RDY')
       
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database) 
    

