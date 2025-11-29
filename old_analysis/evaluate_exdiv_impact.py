#------------------------------------------------------------
# Filename  : evaluate_exdiv_impact.py
# Project   : ava
#
# Descr     : This file contains code to loop through tickers to see the effect
#             of the div date on the price
#
# Params    : database
#             inv_ticker
#             start_date
#             end_date
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2018-10-01   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava r
# 2021-12-27 101 MW  Was written a long time ago and is not 
#                    valid Oracle SQL so converting date routines
#                    to Oracle ones
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

import pandas as pd
from utils.utils_database import close_db, open_db, sql_select_all, sql_select_one
from utils.config import DEBUG, REPORT_FOLDER_OSX, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_dates import previous_TSX_bus_date
from utils.utils_dataframes import normalize_df, tidyup_df


def evaluate_exdiv_impact(database, inv_ticker, start_date, end_date):  
    
    dividends_df = get_exdiv_dates(database   = database,
                                   inv_ticker = inv_ticker, 
                                   start_date = start_date, 
                                   end_date   = end_date)
    
    if not(dividends_df.empty):
        
        print('dividends found')
        
        # before sort extract the 'good' results
        good_df = dividends_df[dividends_df['profit'] > 3]
        print('good_df', good_df)
        # sort in descending profit order
        best_results_df = good_df.sort_values(by='profit', ascending=False)
        print('best_results_df', best_results_df)
        print(best_results_df.to_string(columns=["inv_ticker","as_of_date","exdiv_date","div_per_share","start_price","end_price","profit"]))
        
        # write whole results to file
        output_filename = REPORT_FOLDER_OSX + 'evaluate_exdiv_impact.txt'
        f = open(output_filename,'w')
        
        print(best_results_df.to_string(columns=["inv_ticker","as_of_date","exdiv_date","div_per_share","start_price","end_price","profit"]), 
              file=f)
            
    return
       


def add_to_df(in_df, this_inv_ticker,  this_as_of_date, this_exdiv_date, this_price_per_share, this_start_price, this_end_price):

    #print(' ')
    #print('in add_to_df')
    
    this_as_of_date_as_string = this_as_of_date.strftime('%Y-%m-%d-%H:%M:%S')
    this_investment           = this_inv_ticker 
    this_index                = this_inv_ticker + '_' + this_as_of_date_as_string
    temp                      = pd.DataFrame({'investment': this_investment}, index=[this_index] )
    temp['as_of_date']        = this_as_of_date
    temp['exdiv_date']        = this_exdiv_date
    temp['inv_ticker']        = this_inv_ticker
    temp['div_per_share']     = float(this_price_per_share)
    temp['start_price']       = float(this_start_price)
    temp['end_price']         = float(this_end_price)
    temp['profit']            = (float(this_end_price) - float(this_start_price) + float(this_price_per_share))
    print('adding_record to add_to_df', temp)
    in_df = pd.concat([in_df, temp], sort=False)
    
    return in_df
    

def get_exdiv_dates(database, inv_ticker, start_date, end_date):

    dividends_df = pd.DataFrame()     
    
    dividends = get_div_recs_in_range(database    = database,
                                      inv_ticker = inv_ticker, 
                                      start_date = start_date, 
                                      end_date   = end_date)
    
    for (this_inv_ticker, this_as_of_date, this_div_per_share) in dividends:
        if this_div_per_share == None:
            this_div_per_share = float(0)
            
        # estimate the ex div date as 2 business days in the TSX
        one_day_before  = previous_TSX_bus_date(this_as_of_date)
        this_exdiv_date = previous_TSX_bus_date(one_day_before)
        
        #print('this_inv_ticker, this_as_of_date, this_div_per_share,one_day_before, this_exdiv_date',\
        #      this_inv_ticker, this_as_of_date, this_div_per_share,one_day_before, this_exdiv_date )    
        datetime_of_last_price_before = get_last_price_before_exdiv_date(database   = database,
                                                                         inv_ticker = this_inv_ticker, 
                                                                         exdiv_date = this_exdiv_date)
        
        if datetime_of_last_price_before is not None:
            
            start_datetime_as_string = datetime_of_last_price_before[0].strftime('%Y-%m-%d %H:%M:%S')
            
            start_midpoint_price = get_midpoint_price_for_start_date(database                = database,
                                                                     inv_ticker              = this_inv_ticker, 
                                                                     this_datetime_as_string = start_datetime_as_string)
            
            if start_midpoint_price != None:
                start_midpoint_price = start_midpoint_price[0]
            else:
                start_midpoint_price = float(0)
                
            datetime_of_last_price_after = get_first_price_after_as_of_date(database   = database,
                                                                            inv_ticker = this_inv_ticker, 
                                                                            as_of_date = this_as_of_date)
            
            #print('datetime_of_last_price_after = ', datetime_of_last_price_after)
            if datetime_of_last_price_after is not None:
                end_datetime_as_string = datetime_of_last_price_after[0].strftime('%Y-%m-%d %H:%M:%S')
                
                end_midpoint_price = get_midpoint_price_for_end_date(database                = database,
                                                                     inv_ticker              = this_inv_ticker, 
                                                                     this_datetime_as_string = end_datetime_as_string)
                
                if end_midpoint_price != None:
                    end_midpoint_price = end_midpoint_price[0]
                else:
                    end_midpoint_price = float(0)
                
            
                #this_avg_spread = get_avg_spread_in_range(inv_ticker=this_inv_ticker, exdiv_date=this_exdiv_date)
                
                #if this_avg_spread == None:
                    #this_avg_spread = float(0)
             
                dividends_df = add_to_df(in_df                = dividends_df, 
                                         this_inv_ticker      = this_inv_ticker,  
                                         this_as_of_date      = this_as_of_date, 
                                         this_exdiv_date      = this_exdiv_date,
                                         this_price_per_share = this_div_per_share, 
                                         this_start_price     = start_midpoint_price, 
                                         this_end_price       = end_midpoint_price 
                                         #this_avg_spread     = this_avg_spread
                                         )
                

        
    #print('==========================================================')
    #print('dividends_df',dividends_df)
    #print('==========================================================')
    #print(' ')
    
    return dividends_df


def get_avg_spread_in_range(database, inv_ticker, exdiv_date):
  
    # NB this sql needs updating for oracle
    
    sql_statement =  "SELECT AVG(h1.hmd_last_ask_price - h1.hmd_last_bid_price) as this_avg_spread "
    sql_statement += " FROM  ims_hist_mkt_data h1  "
    sql_statement += " WHERE h1.hmd_inv_ticker = '%s' " % (inv_ticker)
    sql_statement += " AND   h1.hmd_end_datetime between "
    sql_statement += "       to_date('%s', 'YYYY-MM-DD HH24:MI:SS') - 3 " % (exdiv_date)
    sql_statement += " AND   to_date('%s', 'YYYY-MM-DD HH24:MI:SS') + 3 " % (exdiv_date)
    
    if DEBUG:
        print("get_avg_spread_in_range")
        print(sql_statement)
    
    avg_spread = sql_select_one(database, sql_statement)[0]
    
    if DEBUG:
        print(avg_spread)
        
    return avg_spread


def get_div_recs_in_range(database, inv_ticker, start_date, end_date):
  
    sql_statement =  "SELECT div_inv_ticker as this_inv_ticker, "
    sql_statement += " div_as_of_date as this_as_of_date, "
    sql_statement += " div_per_share as this_div_per_share "
    sql_statement += " FROM ims_dividends "
    sql_statement += " WHERE div_inv_ticker LIKE '%s' " % (inv_ticker)
    sql_statement += " and div_report_type = 'A' "
    sql_statement += " and div_period = '3M' "
    sql_statement += " AND div_as_of_date between "
    sql_statement += "       to_date('%s', 'YYYY-MM-DD HH24:MI:SS')  " % (start_date)
    sql_statement += " AND   to_date('%s', 'YYYY-MM-DD HH24:MI:SS')  " % (end_date)
    sql_statement += " ORDER BY div_inv_ticker, div_as_of_date " 
    
    if DEBUG:
        print("get_exdiv_recs_in_range")

    exdivs = sql_select_all(database, sql_statement)
  
    if DEBUG:
        print(exdivs)
        
    return exdivs


def get_first_price_after_as_of_date(database, inv_ticker, as_of_date):
    
    # needs update for oracle conversion
  
    sql_statement =  "SELECT MIN(h1.hmd_end_datetime) as end_datetime "
    sql_statement += " FROM ims_hist_mkt_data h1 "
    sql_statement += " WHERE h1.hmd_inv_ticker = '%s' " % (inv_ticker)
    sql_statement += " AND h1.hmd_end_datetime "
    sql_statement += " >=  to_date('%s', 'YYYY-MM-DD HH24:MI:SS') +3 " % (as_of_date)
    sql_statement += " GROUP BY h1.hmd_inv_ticker"
    
    if DEBUG:
        print("get_first_price_after_as_of_date")

    price_after_exdiv = sql_select_one(database, sql_statement)
    
    if DEBUG:
        print(price_after_exdiv)
        
    return price_after_exdiv


def get_last_price_before_exdiv_date(database, inv_ticker, exdiv_date):
  
    # Needs update for oracle conversion
    
    sql_statement =  "SELECT MAX(h1.hmd_start_datetime) as start_datetime "
    sql_statement += " FROM ims_hist_mkt_data h1 "
    sql_statement += " WHERE h1.hmd_inv_ticker = '%s' " % (inv_ticker)
    sql_statement += " AND to_date(h1.hmd_start_datetime,'YYYY-MM-DD HH24:MI:SS') "
    sql_statement += " <= to_date('%s',                  'YYYY-MM-DD HH24:MI:SS') + 3 " % (exdiv_date)
    sql_statement += " GROUP BY hmd_inv_ticker"
    
    if DEBUG:
        print("get_last_price_before_as_of_date")

    price_before_as_of = sql_select_one(database, sql_statement)
    
    if DEBUG:
        print(price_before_as_of)
        
    return price_before_as_of


def get_midpoint_price_for_end_date(database, inv_ticker, this_datetime_as_string):
  
    sql_statement =  "SELECT h1.hmd_last_bid_price + ((h1.hmd_last_ask_price - h1.hmd_last_bid_price)/2) as midpoint_price "
    sql_statement += " FROM ims_hist_mkt_data h1 "
    sql_statement += " WHERE h1.hmd_inv_ticker = '%s' " % (inv_ticker)
    sql_statement += " AND h1.hmd_end_datetime LIKE '%s" % (this_datetime_as_string)
    sql_statement += "%'"
    
    if DEBUG:
        print("get_midpoint_price_for_start_date")

    midpoint_price = sql_select_one(database, sql_statement)
    
    if DEBUG:
        print(midpoint_price)
        
    return midpoint_price



def get_midpoint_price_for_start_date(database, inv_ticker, this_datetime_as_string):
  
    sql_statement =  "SELECT h1.hmd_last_bid_price + ((h1.hmd_last_ask_price - h1.hmd_last_bid_price)/2) as midpoint_price "
    sql_statement += " FROM ims_hist_mkt_data h1 "
    sql_statement += " WHERE h1.hmd_inv_ticker = '%s' " % (inv_ticker)
    sql_statement += " AND h1.hmd_start_datetime LIKE '%s" % (this_datetime_as_string)
    sql_statement += "%'"
    
    if DEBUG:
        print("get_midpoint_price_for_start_date")

    midpoint_price = sql_select_one(database, sql_statement)
    
    if DEBUG:
        print(midpoint_price)
        
    return midpoint_price

       
if __name__ == "__main__":
    
    print("Started evaluate_exdiv_impact")
    print(" ")
    print("Open db")
    
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
   

    #this_inv_ticker  = 'CM.TO'
    this_inv_ticker = '%'
    this_start_date  = '2018-01-01'
    this_end_date    = '2020-01-26'
    
    evaluate_exdiv_impact(database   = database,   
                          inv_ticker = this_inv_ticker, 
                          start_date = this_start_date, 
                          end_date   = this_end_date)  
    
    print("Close database")
    
    close_db(database = database)   
    
    print("Finished evaluate_exdiv_impact") 


