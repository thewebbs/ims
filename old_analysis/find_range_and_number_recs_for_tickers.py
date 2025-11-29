#------------------------------------------------------------
# Filename  : find_range_and_number_recs_for_ticker.py
# Project   : ava
#
# Descr     : This file contains code to find trading opportunities
#
# Params    : database
#             inv_ticker
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-12-10   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


from decimal import Decimal
import matplotlib.pyplot as plt
from database.db_objects.ImsInvestmentDB import get_all_loading_investments, is_on_TO_and_NYSE
from database.db_objects.ImsHistMktDataDB import get_earliest_start_datetime_for_ticker, get_latest_start_datetime_for_ticker, get_number_records_between_dates_for_ticker
from database.db_objects.ImsTradeOppsDB import ImsTradeOppsDB
from datetime import datetime
from infrastructure.blackboard.load_todos_manager import create_batch_load_todos
from operator import itemgetter
import pandas as pd
from utils.config import DEBUG, MIN_TRADEOPP_SCALE, MIN_WIBBLINESS_THRESHOLD, REPORT_FOLDER_OSX
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import open_db, close_db 
from utils.utils_dataframes import format_changes_df


def find_range_and_number_recs_for_ticker(database, inv_ticker):

    ticker_list = get_all_loading_investments(database = database, 
                                              inv_ticker = inv_ticker)
    
    results_list = []
    for inv_ticker_tuple in ticker_list:
        inv_ticker = inv_ticker_tuple[0]
    
        earliest_start_datetime = get_earliest_start_datetime_for_ticker(database       = database, 
                                                                         hmd_inv_ticker = inv_ticker)
        
        
        if earliest_start_datetime != None: 

            latest_end_datetime = get_latest_start_datetime_for_ticker(database       = database, 
                                                                       hmd_inv_ticker = inv_ticker)
        
        
            number_records = get_number_records_between_dates_for_ticker(database       = database, 
                                                                         hmd_inv_ticker = inv_ticker, 
                                                                         start_datetime = earliest_start_datetime, 
                                                                         end_datetime   = latest_end_datetime)
    
            
            ticker_is_on_TO_and_NYSE = is_on_TO_and_NYSE(database   = database,
                                                         inv_ticker = inv_ticker)
            
            if ticker_is_on_TO_and_NYSE is None:
                ticker_is_on_TO_and_NYSE = 'No'
            else:
                ticker_is_on_TO_and_NYSE = 'Yes'
                
            
            this_dict = {"inv_ticker": inv_ticker, 
                         "startdatetime": earliest_start_datetime, 
                         "enddatetime": latest_end_datetime, 
                         "num_recs": number_records,
                         "num_days": round(number_records/390),
                         "on_both": ticker_is_on_TO_and_NYSE}
            
            results_list.append(dict(this_dict))
            
        #else:
            #print(inv_ticker,'not loaded')
    
    newlist = sorted(results_list, key=itemgetter('num_recs'), reverse=True )
       
    for dic in newlist:
        this_inv_ticker = dic['inv_ticker']
        this_num_days = dic['num_days']
        this_is_on_TO_and_NYSE = dic['on_both']
        this_start_datetime = dic['startdatetime']
        this_end_datetime = dic['enddatetime']
        if this_num_days > 100:
            print(this_inv_ticker, this_num_days, 'Days', 'on both?', this_is_on_TO_and_NYSE)
            
            
            # create one or more streak todo records for this ticker for the whole period 

            str_start_datetime = datetime.strftime(this_start_datetime,"%Y-%m-%d %H:%M:%S")
            str_end_datetime = datetime.strftime(this_end_datetime,"%Y-%m-%d %H:%M:%S")            

            create_batch_load_todos(database           = database,
                                    inv_ticker         = this_inv_ticker, 
                                    rec_type           = 'STREAK', 
                                    freq_type          = 'NA', 
                                    str_start_datetime = str_start_datetime, 
                                    str_end_datetime   = str_end_datetime, 
                                    progress_status    = 'HOLDFORNOW', 
                                    load_priority      = 1)   
            
 
    return newlist

    
if __name__ == "__main__":
    
    print("Started find_range_and_number_recs_for_ticker")
    print(" ")
    print("Open Database")
        
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)   
    
    inv_ticker = '%'
    result = find_range_and_number_recs_for_ticker(database   = database, 
                                                   inv_ticker = inv_ticker)

                     
    print("Close database")
    close_db(database = database)   
    
    print("Finished find_range_and_number_recs_for_ticker") 
    
    
 
