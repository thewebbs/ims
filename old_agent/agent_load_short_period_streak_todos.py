#------------------------------------------------------------
# Filename  : agent_load_short_period_streak_todos.py
# Project   : ava
#
# Descr     : This holds routine to load ims_load_todos with requests for
#             streak detection - based on agent_load_short_period_streak_todos 
#             but designed for specific ranges of dates and specific tickers
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-12-16   1 MW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


from datetime import datetime, timedelta, time
from infrastructure.blackboard.load_todos_manager import create_batch_load_todos
from database.db_objects.ImsInvestmentDB import get_investment
from utils.config import DEBUG, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD, REPORT_FOLDER_OSX
from utils.utils_database import close_db, open_db
from utils.utils_dates import daterange


def agent_load_short_period_streak_todos(database, rec_type, freq_type, inv_ticker, inv_sec_name,
                                         str_start_datetime, str_end_datetime, progress_status,
                                         inv_load_priority):

    # first verify investment
    
    result = get_investment(database   = database,
                            inv_ticker = inv_ticker)
    
    if result == None:
        print('Ticker',inv_ticker,'not on ims_investments_table')
    
    else:
    
        progress = 'Creating ims_load_todos %s records for %s from %s to %s' % (rec_type, inv_ticker, str_start_datetime, str_end_datetime)
        print(progress)
                
        create_batch_load_todos(database = database,
                                inv_ticker         = inv_ticker, 
                                rec_type           = rec_type, 
                                freq_type          = freq_type, 
                                str_start_datetime = str_start_datetime, 
                                str_end_datetime   = str_end_datetime, 
                                progress_status    = progress_status, 
                                load_priority      = inv_load_priority)

    
    return


if __name__ == "__main__":

    print("Open db")
    print(" ")

    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
    
      
    rec_type           = 'STREAK'
    freq_type          = 'NA'
    inv_ticker         = '%' 
    inv_sec_name       = '%'
        
    str_start_datetime = '2020-01-01 06:30:00' 
    str_end_datetime   = '2020-12-11 23:59:59'
    progress_status    = 'RDYSTRK'
    inv_load_priority = 10
    
    # both NYSE and TO
    #for inv_ticker in ('L.NYSE','TD.NYSE','ATH.NYSE','AXL.NYSE','BX.NYSE','BYD.NYSE','CBL.NYSE','CCS.NYSE','CEF.NYSE','CM.NYSE'): 
    
    # just NYSE
    #for inv_ticker in ('A.NYSE','AA.NYSE','AACS.NYSE','AAL.NYSE','AAMC.NYSE','AAME.NYSE','AAN.NYSE','AAOI.NYSE','AAON.NYSE','AAP.NYSE'): 
    
    #for inv_ticker in ('L.TO','TD.TO','ATH.TO','AXL.TO','BX.TO','BYD.TO','CBL.TO','CCS.TO','CEF.TO','CM.TO'): 
    inv_ticker ='CEF.TO'
    
    agent_load_short_period_streak_todos(database           = database, 
                                         rec_type           = rec_type, 
                                         freq_type          = freq_type,
                                         inv_ticker         = inv_ticker, 
                                         inv_sec_name       = inv_sec_name,
                                         str_start_datetime = str_start_datetime, 
                                         str_end_datetime   = str_end_datetime, 
                                         progress_status    = progress_status,
                                         inv_load_priority  = inv_load_priority)  
    
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database)  
    

