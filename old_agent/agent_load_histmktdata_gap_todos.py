#------------------------------------------------------------
# Filename : agent_load_histmktdata_gap_todos.py
# Project  : ava
#
# Descr    : This finds gaps in the historic market data and creates load_todos 
#            to fill the gaps
#
# Params    : database
#             inv_ticker
#             freq_type
#             start_date
#
# History  :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2019-12-30   1 MW  Initial write
# ...
# 2021-07-16     DW  Moved to ILS-ava 
# 2021-08-25 100 DW  Added version
# 2022-11-05 200 DW  Reorg 
#------------------------------------------------------------


from datetime import datetime, timedelta
from infrastructure.blackboard.load_todos_manager import create_batch_load_todos
from dateutil.relativedelta import relativedelta

from database.db_objects.ImsExchangeDB import get_open_days, get_exchange
from database.db_objects.ImsInvestmentDB import get_all_loading_investments

from utils.config import DEBUG, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db, sql_select_one

        
def agent_load_histmktdata_gap_todos(database, inv_ticker, freq_type, start_date):

    result = 'SUCCESS'

    # cycle through tickers with to load status of Y - find and fill the gaps for each

    tickers = get_all_loading_investments(database   = database, 
                                          inv_ticker = inv_ticker)
    
    for ticker_tuple in tickers:
        inv_ticker = ticker_tuple[0]
        
        rec_type = 'HISTMKTDATA_BID'
        result = agent_histmktdata_fill_gaps_for_rec_type(database   = database, 
                                                          inv_ticker = inv_ticker, 
                                                          freq_type  = freq_type, 
                                                          rec_type   = rec_type, 
                                                          start_date = start_date)
        
        rec_type = 'HISTMKTDATA_ASK'
        result = agent_histmktdata_fill_gaps_for_rec_type(database   = database, 
                                                          inv_ticker = inv_ticker, 
                                                          freq_type  = freq_type, 
                                                          rec_type   = rec_type, 
                                                          start_date = start_date)
        
        rec_type = 'HISTMKTDATA_TRADES'
        result = agent_histmktdata_fill_gaps_for_rec_type(database   = database, 
                                                          inv_ticker = inv_ticker, 
                                                          freq_type  = freq_type, 
                                                          rec_type   = rec_type, 
                                                          start_date = start_date)

    return result


def agent_histmktdata_fill_gaps_for_rec_type(database, inv_ticker, freq_type, rec_type, start_date):

    result = 'SUCCESS'
    
    # find earliest and latest histmktdata record for this ticker
    
    ticker_info = find_first_last_histmktdata_priority(database   = database, 
                                                       inv_ticker = inv_ticker, 
                                                       freq_type  = freq_type)
    
    # Add this next line because sometimes get an error
    if ticker_info != None:
        
        this_ticker_start_date = ticker_info[0]
    
        # use this start date only if start date not specified
        
        if start_date == '%':
            start_date = this_ticker_start_date
         
        end_date        = ticker_info[1]
        load_priority   = ticker_info[2]
        progress_status = 'GAP'
        
        # find the days that this exchange is open
      
        exc_symbol = get_exchange(inv_ticker = inv_ticker)   
          
        open_days  = get_open_days(database   = database,
                                   exc_symbol = exc_symbol, 
                                   start_date = start_date, 
                                   end_date   = end_date)
                
        # iterate though from earliest to latest business dates to identify any missing
        
        for next_date in open_days:
            
            data_exists = find_this_histmkt_data(database   = database, 
                                                 inv_ticker = inv_ticker, 
                                                 freq_type  = freq_type, 
                                                 this_date  = next_date)
            
            if not(data_exists):
                
                print('Found missing record', inv_ticker, freq_type, rec_type, next_date)
       
                # create 1 load to do record - one for this ticker, freq_type, rec_type and the missing date     
    
                start_datetime = datetime.strftime(next_date,"%Y-%m-%d") + " 00:00:00"
                end_datetime   = datetime.strftime((next_date + relativedelta(days=1)),"%Y-%m-%d") + " 00:00:00"
                
                create_batch_load_todos(database            = database, 
                                        inv_ticker          = inv_ticker, 
                                        rec_type            = rec_type, 
                                        freq_type           = freq_type, 
                                        str_start_datetime  = start_datetime, 
                                        str_end_datetime    = end_datetime, 
                                        progress_status     = progress_status, 
                                        load_priority       = load_priority)
                
    return result


def find_first_last_histmktdata_priority(database, inv_ticker, freq_type):
    
    # finds the earliest hmd_start_datetime and latest hmd_end_datetime
    # for this ticker and freq_type combination
    
    sql_statement = "SELECT MIN(HMD_START_DATETIME), MAX(HMD_END_DATETIME), INV_LOAD_PRIORITY "
    sql_statement += " FROM IMS_HIST_MKT_DATA, IMS_INVESTMENTS "
    sql_statement += " WHERE HMD_INV_TICKER = INV_TICKER "
    sql_statement += " AND   HMD_INV_TICKER = '%s' " % (inv_ticker)
    sql_statement += " AND   HMD_FREQ_TYPE  = '%s' " % (freq_type)
    sql_statement += " GROUP BY INV_LOAD_PRIORITY "
    
    ticker_info = sql_select_one(database      = database,
                                 sql_statement = sql_statement)
    
    if DEBUG:
        print(ticker_info)
          
    return ticker_info


def find_this_histmkt_data(database, inv_ticker, freq_type, this_date):
    
    # finds if there is a histmktdata record for this ticker, freq_type and date 
    
    sql_statement = "SELECT 1 FROM IMS_HIST_MKT_DATA "
    sql_statement += " WHERE HMD_INV_TICKER           = '%s' " % (inv_ticker)
    sql_statement += " AND   HMD_FREQ_TYPE            = '%s' " % (freq_type)
    sql_statement += " AND   HMD_START_DATETIME = to_date('%s', 'YYYY-MM-DD HH24:MI:SS' ) " % (this_date)
    sql_statement += " UNION "
    sql_statement += " SELECT 0 "
    sql_statement += " FROM DUAL "
    sql_statement += " WHERE NOT EXISTS ( "
    sql_statement += " SELECT 1 FROM IMS_HIST_MKT_DATA"
    sql_statement += "  "
    sql_statement += " WHERE HMD_INV_TICKER           = '%s' " % (inv_ticker)
    sql_statement += " AND   HMD_FREQ_TYPE            = '%s' " % (freq_type)
    sql_statement += " AND   HMD_START_DATETIME = to_date('%s', 'YYYY-MM-DD HH24:MI:SS' )" % (this_date)
    sql_statement += " ) "
    
    ticker_result = sql_select_one(database      = database,
                                   sql_statement = sql_statement)[0]
    
    if ticker_result == 1:
        data_exists = True
    else:
        data_exists = False
    
    if DEBUG:
        print(data_exists)
          
    return data_exists
    

if __name__ == "__main__":

    print("Open db")
    print(" ")
    
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
          
    #inv_ticker = '%'
    inv_ticker = 'CM.TO'
    freq_type = '1 min'
    
    # specify start date if want to pre-fill or leave as % to start from current
    #start_date = '%'
    
    start_date = '2020-01-01'
    
    result = agent_load_histmktdata_gap_todos(database   = database, 
                                              inv_ticker = inv_ticker, 
                                              freq_type  = freq_type,
                                              start_date = start_date)
    
    print('Result at end = ', result)
    
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database) 



    
    
    

