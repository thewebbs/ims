#------------------------------------------------------------
# Filename  : EarlyMorning.py
# Project   : ava
#
# Descr     : This file contains code to process streaks in historical
#             market data. This is based on the old DetectStreaks but
#             is now a class of it's own
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-01-05   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2021-12-26 101 MW  Removed ils_assist. throughout
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from datetime import datetime
from database.db_objects.ImsAnalysisResultDB import delete_analysis_results, ImsAnalysisResultDB
from database.db_objects.ImsInvestmentDB import get_last_trading_date_for_ticker
from utils.config import DEBUG, EARLY_AM_MINUTES_THRESHOLD
from utils.utils_database import sql_select_one
from utils.utils_dates import previous_TSX_bus_date

  
  
def get_overnight_changes(database, inv_ticker, trading_date, previous_trading_date):
    
    sql_statement = "select  today.hmd_inv_ticker                                       as inv_ticker,  "
    sql_statement += "       today.hmd_start_datetime as trading_date, "
    sql_statement += "       yesterday.hmd_last_ask_price                               as closing_ask, "
    sql_statement += "       today.hmd_start_ask_price                                  as opening_ask, "
    sql_statement += "       yesterday.hmd_last_bid_price                               as closing_bid, "
    sql_statement += "       today.hmd_start_bid_price                                  as opening_bid, "
    sql_statement += "       (today.hmd_start_ask_price - yesterday.hmd_last_ask_price) as overnight_change_ask, "
    sql_statement += "       (today.hmd_start_bid_price - yesterday.hmd_last_bid_price) as overnight_change_bid "
    sql_statement += " from  ims_hist_mkt_data today, "
    sql_statement += "       ims_hist_mkt_data yesterday "
    sql_statement += " where yesterday.hmd_inv_ticker     = today.hmd_inv_ticker  "
    sql_statement += " and   today.hmd_start_datetime     = to_date('%s', 'YYYY-MM-DD HH24:MI:SS') " % (trading_date)
    sql_statement += " and   yesterday.hmd_start_datetime = to_date('%s', 'YYYY-MM-DD HH24:MI:SS') " % (previous_trading_date)
    sql_statement += " and   today.hmd_inv_ticker         = '%s' "  % (inv_ticker)
    
    if DEBUG:
        print("get_overnight_changes")

    result = sql_select_one(database, sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_intra_day_changes(database, inv_ticker, trading_date):
    
    # calculate the early morning window using the config parameter
    start_datetime = trading_date.strftime('%Y-%m-%d') + ' 06:30:00'
    interval       = 30 + EARLY_AM_MINUTES_THRESHOLD
    end_datetime   = trading_date.strftime('%Y-%m-%d') + " 06:" + str(interval) + ":00"
    
    sql_statement = "select  MAX(hmd_start_ask_price) as highest_ask, "
    sql_statement += "       MIN(hmd_start_ask_price) as lowest_ask, "
    sql_statement += "       MAX(hmd_start_bid_price) as highest_bid, "
    sql_statement += "       MIN(hmd_start_bid_price) as lowest_bid, "
    sql_statement += "       MAX(hmd_start_ask_price) - MIN(hmd_start_ask_price) as morning_change_ask, "
    sql_statement += "       MAX(hmd_start_bid_price) - MIN(hmd_start_bid_price) as morning_change_bid "
    sql_statement += " from    ims_hist_mkt_data "
    sql_statement += " where   hmd_start_datetime between to_date('%s', 'YYYY-MM-DD HH24:MI:SS') and to_date('%s', 'YYYY-MM-DD HH24:MI:SS') " % (start_datetime, end_datetime)
    sql_statement += " and     hmd_inv_ticker     = '%s' "  % (inv_ticker)
    
    if DEBUG:
        print("get_intra_day_changes")

    result = sql_select_one(database, sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


#
# Class definition
#

class EarlyMorning:

    def __init__(self, database, analysis_type, inv_ticker, trading_date, previous_trading_date, 
                 closing_bid, closing_ask, opening_bid, opening_ask, overnight_change_bid, overnight_change_ask,
                 highest_bid, highest_ask, lowest_bid, lowest_ask, morning_change_bid, morning_change_ask):

        self.database              = database
        self.analysis_type         = analysis_type
        self.inv_ticker            = inv_ticker
        self.trading_date          = trading_date
        self.previous_trading_date = previous_trading_date
        self.closing_bid           = closing_bid
        self.closing_ask           = closing_ask
        self.opening_bid           = opening_bid
        self.opening_ask           = opening_ask
        self.overnight_change_bid  = overnight_change_bid
        self.overnight_change_ask  = overnight_change_ask
        self.highest_bid           = highest_bid
        self.highest_ask           = highest_ask
        self.lowest_bid            = lowest_bid
        self.lowest_ask            = lowest_ask
        self.morning_change_bid    = morning_change_bid
        self.morning_change_ask    = morning_change_ask
        
        return 
    

    def delete_existing_analysis_results(self):
    
        # because the key sequence is auto increment we set it to null here
    
        result = delete_analysis_results(database          = self.database, 
                                         iar_iat_type      = self.analysis_type, 
                                         iar_inv_ticker    = self.inv_ticker, 
                                         iar_date          = self.trading_date)
            
        if DEBUG:
            print('result after delete_existing_analysis_results ', result)
    
        return


    def collect_early_morning_stats(self):

        self.overnight_change_bid = 0
        self.overnight_change_ask = 0
        self.morning_change_bid   = 0
        self.morning_change_ask   = 0
        
        print('=======================================================================')
        print('Processing for ', self.inv_ticker, ' for ', self.analysis_type, 'for', self.trading_date)

        # first delete any existing
        
        self.delete_existing_analysis_results()

        # next get the last trading date for this ticker and set the time to market open time
        
        self.trading_date = get_last_trading_date_for_ticker(database   = self.database, 
                                                             inv_ticker = self.inv_ticker)[0]
        
        trading_date_set_time = datetime.strftime(self.trading_date,'%Y-%m-%d') + ' 06:30:00'
        self.trading_date = datetime.strptime(trading_date_set_time,'%Y-%m-%d %H:%M:%S')
        
        # get the previous business date on the TSX calendar
        # then set the time component to market closing on that day
        
        previous_trading_date = previous_TSX_bus_date(this_business_date = self.trading_date)
        self.previous_trading_date = previous_trading_date.replace(hour=12, minute=59)
        
        # now start collecting the early morning stats
        result = self.collect_analysis_results()
         
        # record early morning stats
        self.record_analysis_results()
    
        return


    def collect_analysis_results(self):
        
        
        overnights = get_overnight_changes(database              = self.database, 
                                           inv_ticker            = self.inv_ticker, 
                                           trading_date          = self.trading_date, 
                                           previous_trading_date = self.previous_trading_date)
        
        if overnights != None:
            result = 'SUCCESS'
            self.inv_ticker = overnights[0]
            self.trading_date = overnights[1]
            self.closing_ask = overnights[2]
            self.opening_ask = overnights[3]
            self.closing_bid = overnights[4]
            self.opening_bid = overnights[5]
            self.overnight_change_ask = overnights[6]
            self.overnight_change_bid = overnights[7]
            
            if DEBUG:
                print('overnights', self.inv_ticker, self.trading_date, 
                      self.closing_ask, self.opening_ask, self.closing_bid, 
                      self.opening_bid, self.overnight_change_ask, self.overnight_change_bid)
        
            intradays = get_intra_day_changes(database     = self.database, 
                                              inv_ticker   = self.inv_ticker, 
                                              trading_date = self.trading_date)
               
            
            if intradays != None:
                result = 'SUCCESS'
                
                self.highest_ask = intradays[0]
                self.lowest_ask = intradays[1]
                self.highest_bid = intradays[2]
                self.lowest_bid = intradays[3]
                self.morning_change_ask = intradays[4]
                self.morning_change_bid = intradays[5]
                    
                if DEBUG:
                    print('intradays', self.lowest_ask, 
                          self.highest_bid, self.lowest_bid, 
                          self.morning_change_ask, self.morning_change_bid)
        else:
            result = 'ERROR'
        
        return result


    def record_analysis_results(self):
    
        # work out the date less the time for the summary record
        
        analysis_result = ImsAnalysisResultDB(database        = self.database,
                                              iar_iat_type    = self.analysis_type, 
                                              iar_inv_ticker  = self.inv_ticker, 
                                              iar_date        = self.trading_date, 
                                              iar_value1      = self.closing_bid, 
                                              iar_value2      = self.closing_ask, 
                                              iar_value3      = self.opening_bid, 
                                              iar_value4      = self.opening_ask, 
                                              iar_value5      = self.overnight_change_bid, 
                                              iar_value6      = self.overnight_change_ask, 
                                              iar_value7      = self.highest_bid, 
                                              iar_value8      = self.highest_ask, 
                                              iar_value9      = self.lowest_bid, 
                                              iar_value10     = self.lowest_ask, 
                                              iar_value11     = self.morning_change_bid, 
                                              iar_value12     = self.morning_change_ask
                                              )
        
        analysis_result.insert_DB()
            
        return 

