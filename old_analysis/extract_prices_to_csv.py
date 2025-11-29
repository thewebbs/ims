#------------------------------------------------------------
# Filename  : extract_prices_to_csv.py
# Project   : ava
#
# Descr     : This file contains code to extract prices for a ticker in a timeframe to csv file
#
# Params    : ticker (or %)
#             start_datetime
#             end_datetime
#             filename_no_suffix including pathbut no suffix
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-01-24   1 MW  Initial write
# ...
# 2021-09-05 100 DW  Added version and moved to ILS-ava
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


from database.db_objects.ImsHistMktDataDB import get_all_hist_mkt_data_in_range
from utils.config import DEBUG, REPORT_FOLDER_OSX, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import open_db, close_db, sql_select_all
#from platform import system
from utils.utils_file import write_rec_to_csv
from utils.utils_sys import get_sys_params


def extract_prices_to_csv(database, inv_ticker, str_start_datetime, str_end_datetime, filename_no_suffix):
    
    print("Started extract_prices_to_csv")
    print(" ")
    
    column_list = [["HMD_INV_TICKER"],["HMD_START_DATETIME"],["HMD_END_DATETIME"],["HMD_FREQ_TYPE"],\
                   ["HMD_START_BID_PRICE"],["HMD_HIGHEST_BID_PRICE"],["HMD_LOWEST_BID_PRICE"],["HMD_LAST_BID_PRICE"],\
                   ["HMD_START_ASK_PRICE"],["HMD_HIGHEST_ASK_PRICE"],["HMD_LOWEST_ASK_PRICE"],["HMD_LAST_ASK_PRICE"],\
                   ["HMD_FIRST_TRADED_PRICE"],["HMD_HIGHEST_TRADED_PRICE"],["HMD_LOWEST_TRADED_PRICE"],\
                   ["HMD_LAST_TRADED_PRICE"],["HMD_TOTAL_TRADED_VOLUME"]]
    
    freq_type = '1 min'
    price_rows = get_all_hist_mkt_data_in_range(database           = database,
                                                hmd_inv_ticker     = inv_ticker, 
                                                hmd_start_datetime = str_start_datetime, 
                                                hmd_end_datetime   = str_end_datetime, 
                                                hmd_freq_type      = freq_type)
    
    if price_rows != None:

        write_rec_to_csv(filename_no_suffix, column_list, price_rows)

        print("Finished extract_prices_to_csv") 
    
    else:
        
        print("No data found to extract_prices_to_csv")
        
    return
    

if __name__ == "__main__":
    
    (num_args, args_list) = get_sys_params()
    
    
    if num_args == 0:
        inv_ticker             = 'CM.TO'
        str_start_datetime     = '2020-01-21 06:30:00'
        str_end_datetime       = '2020-01-21 12:59:59'
        filename_no_suffix     = REPORT_FOLDER_OSX + 'extract_prices_CM'
        
        print('filename_no_suffix here', filename_no_suffix)
        
    else:
        if num_args == 4:
            inv_ticker         = args_list[0]
            str_start_datetime = args_list[1]
            str_end_datetime   = args_list[2] 
            filename_no_suffix = args_list[3] 
              
        
    if (num_args != 4 and num_args != 0):
        print("ERROR extract_prices_to_csv.py - wrong number of args provided expected 4")
        print("Number args: ", num_args)
        print("Arg list: ", args_list)
        
    else:
        
        print("Open Database")
   
        database = open_db(host        = DB_HOST, 
                           port        = DB_PORT, 
                           tns_service = DB_TNS_SERVICE, 
                           user_name   = DB_USER_NAME, 
                           password    = DB_PASSWORD)    
            
        extract_prices_to_csv(database           = database,
                              inv_ticker         = inv_ticker, 
                              str_start_datetime = str_start_datetime, 
                              str_end_datetime   = str_end_datetime, 
                              filename_no_suffix = filename_no_suffix)
    
        print("Close database")
        
        close_db(database = database)   

    
 
