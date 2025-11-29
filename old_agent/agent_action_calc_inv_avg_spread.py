#------------------------------------------------------------
# Filename  : agent_action_calc_inv_avg_spread.py
# Project   : ava
#
# Descr     : This holds routines to load calculate and store the average bid offer
#             spread in a given date range
#
# Params    : database
#             start_datetime
#             end_datetime
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-08-11   1 MW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-10-29 200 DW  Reorg
#------------------------------------------------------------


from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db
from database.db_objects.ImsInvestmentDB import get_all_loading_investments, update_avg_spread
from database.db_objects.ImsHistMktDataDB import get_avg_spread_in_date_range
from decimal import Decimal
import datetime
from dateutil.relativedelta import relativedelta


def agent_action_calc_inv_avg_spread(database, start_datetime, end_datetime):
    
    print('===========================================================================================')   
    print(' Start of action_calc_inv_avg_spread processing')
    print(' Between', start_datetime,'and', end_datetime)
    print('===========================================================================================')   

    ticker_list = get_all_loading_investments(database = database, 
                                              inv_ticker = '%')
    
    for inv_ticker_tuple in ticker_list:
        inv_ticker = inv_ticker_tuple[0]
         
        avg_spread = get_avg_spread_in_date_range(database       = database,
                                                  hmd_inv_ticker = inv_ticker,
                                                  start_datetime = start_datetime,
                                                  end_datetime   = end_datetime)
        
        if avg_spread == None:
            avg_spread = Decimal(0)

        if avg_spread > 0:
            result = update_avg_spread(database   = database, 
                                       inv_ticker = inv_ticker, 
                                       avg_spread = avg_spread)
            
    
            print('Updated ticker',inv_ticker, 'with Average spread', avg_spread)
        else:
            print('Did not update ticker',inv_ticker, 'as Average spread is zero')
        
        
    return


if __name__ == "__main__":

    print("Open db")
    print(" ")
    
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
       
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonthStart = first + relativedelta(months=-1)
    start_datetime = lastMonthStart.strftime("%Y-%m-%d 06:30:00")
    
    lastMonthEnd = first - datetime.timedelta(days=1)
    end_datetime = lastMonthEnd.strftime("%Y-%m-%d 12:59:59")
    
    agent_action_calc_inv_avg_spread(database       = database,
                                     start_datetime = start_datetime,
                                     end_datetime   = end_datetime)
       
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database)  
    

