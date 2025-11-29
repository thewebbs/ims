#------------------------------------------------------------
# Filename  : volatility_analysis.py
# Project   : ava
#
# Descr     : This file contains functions relating to analysing volatility
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2016-02-12   1 MW  Initial write
# ...
# 2021-09-05 100 DW  Added version and moved to ILS-ava
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG, MIN_PRICE
from utils.utils_database import sql_select_all
from datetime import date 
from decimal import Decimal
from matplotlib.dates import date2num
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def agg_prices_in_range_from_drive_table(database, run_name, start_date, end_date, check_type):

    # return the pattern for these investments in the drive table for this run, in date range,
    # aggregating by day of week or day of month, depending on check_type

    if check_type == 'W':
        sql_statement = "SELECT pri_inv_ticker, pri_inv_exc_symbol, pri_date, date_format(pri_date,'%w') as priceperiod, "
    else:
        sql_statement = "SELECT pri_inv_ticker, pri_inv_exc_symbol, pri_date, date_format(pri_date,'%d') as priceperiod, "
       
    sql_statement += " pri_adj_close  FROM prices, drive_table "
    sql_statement += " WHERE drt_inv_ticker = pri_inv_ticker "
    sql_statement += " AND drt_inv_exc_symbol = pri_inv_exc_symbol "
    sql_statement += " AND drt_name = '%s' " % (run_name)
    sql_statement += " AND pri_date between '%s' and '%s' " % (start_date, end_date)
    sql_statement += " AND pri_adj_close > '%s' " % (MIN_PRICE)
    sql_statement += " ORDER BY pri_date " 
    
    if DEBUG:
        print("agg_prices_in_range_from_drive_table")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_all_inv(database, start_date, end_date):
    
    # return the tickers for investments in the drive table for this run
    # only return those whose price is above the minimum to ignore penny stocks
    
    sql_statement = "SELECT distinct pri_inv_ticker, pri_inv_exc_symbol FROM prices WHERE pri_date between '%s' and '%s' AND pri_adj_close > '%s'  " \
                     % (start_date, end_date, MIN_PRICE)
    
    if DEBUG:
        print("get_all_inv")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_candlestick_prices_in_range(database, start_date, end_date):
    
    # return the price and adjusted close for all investments within the date range
    # only return those whose price is above the minimum to ignore penny stocks
                                      
    sql_statement = "SELECT can_inv_ticker, can_inv_exc_symbol, can_start_datetime, can_last_traded_price "
    sql_statement += "from candlesticks "
    sql_statement += "where can_start_datetime between '%s' and '%s' "  % (start_date, end_date)
    sql_statement += "and can_last_traded_price is not null "
    sql_statement += "and can_last_traded_price > '%s' "  % (MIN_PRICE)
    sql_statement += "order by can_inv_ticker, can_inv_exc_symbol "

    if DEBUG:
        print("get_candlestick_prices_in_range")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_candlestick_dates_in_range_from_drive_table(database, run_name, start_date, end_date, inv_ticker, inv_exc_symbol):
    
    # return the price and adjusted close for all investment within the date range whose tickers exist in drive table
    
    sql_statement = "SELECT distinct can_end_datetime, dayofweek(can_end_datetime) "
    sql_statement += "from candlesticks, drive_table "
    sql_statement += "where drt_name like '%s' " % (run_name)
    sql_statement += "and drt_inv_ticker = can_inv_ticker "
    sql_statement += "and drt_inv_exc_symbol = can_inv_exc_symbol "
    sql_statement += "and can_inv_ticker like '%s' " % (inv_ticker)
    sql_statement += "and can_inv_exc_symbol like '%s' " % (inv_exc_symbol)
    sql_statement += "and can_end_datetime between '%s' and '%s' "  % (start_date, end_date)
    sql_statement += " AND DATE_FORMAT(can_end_datetime,'%H%i%s') like '125959%' "
    sql_statement += "and can_last_traded_price is not null "
    sql_statement += "order by can_end_datetime "

    if DEBUG:
        print("get_candlestick_dates_in_range_from_drive_table")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_candlestick_prices_in_range_from_drive_table(database, run_name, start_date, end_date, inv_ticker, inv_exc_symbol):
    
    # return the price and adjusted close for all investment within the date range whose tickers exist in drive table
    
    sql_statement = "SELECT can_inv_ticker, can_inv_exc_symbol, can_end_datetime, can_last_traded_price "
    sql_statement += "from candlesticks, drive_table "
    sql_statement += "where drt_name like '%s' " % (run_name)
    sql_statement += "and drt_inv_ticker = can_inv_ticker "
    sql_statement += "and drt_inv_exc_symbol = can_inv_exc_symbol "
    sql_statement += "and can_inv_ticker like '%s' " % (inv_ticker)
    sql_statement += "and can_inv_exc_symbol like '%s' " % (inv_exc_symbol)
    sql_statement += "and can_end_datetime between '%s' and '%s' "  % (start_date, end_date)
    sql_statement += " AND DATE_FORMAT(can_end_datetime,'%H%i%s') like '125959%' "
    sql_statement += "and can_last_traded_price is not null "
    sql_statement += "order by can_inv_ticker, can_inv_exc_symbol, can_end_datetime "
        
    if DEBUG:
        print("get_candlestick_prices_in_range_from_drive_table")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_inv_in_drive_table(database, run_name):
    
    # return the tickers for investments in the drive table for this run
    
    sql_statement = "SELECT drt_inv_ticker, drt_inv_exc_symbol FROM drive_table WHERE drt_name like '%s' " \
                     % (run_name)
    
    if DEBUG:
        print("get_inv_in_drive_table")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_inv_in_portfolio(database, portfolio_name, start_date, end_date):
    
    # return the ticker and exchange symbol of the investment for all investments in the specified PortfolioDB
   
    sql_statement = "SELECT distinct pos_inv_ticker, pos_inv_exc_symbol FROM positions WHERE pos_por_name = '%s' ORDER BY pos_inv_ticker " % (portfolio_name)

    if DEBUG:
        print("get_inv_in_portfolio")
    
    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
  
    if DEBUG:
        print(result)
            
    return result


def get_inv_in_sector(database, sector_name):
    
    # return the ticker and exchange symbol of the investment for all investments in the specified SectorDB
   
    sql_statement = "SELECT inv_ticker, inv_exc_symbol FROM investments WHERE inv_sec_name = '%s' ORDER BY inv_ticker " % (sector_name)

    if DEBUG:
        print("get_inv_in_sector")
    
    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
  
    if DEBUG:
        print(result)
            
    return result


def get_ohlc_in_range_for_inv(database, inv_ticker, inv_exc_symbol, start_date, end_date):
 
    # return the open, high, low, close prices for the investment within the date range
    # reformatted ready for candlestick graph
    
    sql_statement = "SELECT pri_date, pri_open, pri_close, pri_high, pri_low "
    sql_statement += "FROM prices "
    sql_statement += "WHERE pri_inv_ticker = '%s' " % (inv_ticker)
    sql_statement += "AND pri_inv_exc_symbol = '%s' " % (inv_exc_symbol)
    sql_statement += "AND pri_date between '%s' AND '%s' " % (start_date, end_date)
    sql_statement += "order by pri_date "
  
    if DEBUG:
        print("get_ohlc_in_range_for_inv")
    
    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
  
    if DEBUG:
        print(result)
    
    newresult = []
    
    for (pri_date, pri_open, pri_high, pri_low, pri_close) in result:
        
        format_date = date2num(pri_date)
        new_value = format_date, (float(pri_open)), (float(pri_high)), (float(pri_low)), (float(pri_close))
        new_value = format_date, pri_open, pri_high, pri_low, pri_close
        
        newresult.append(new_value)
        
    return newresult


def get_prices_in_range(database, start_date, end_date):
    
    # return the price and adjusted close for all investments within the date range
    # only return those whose price is above the minimum to ignore penny stocks
                                      
    sql_statement = "SELECT pri_inv_ticker, pri_inv_exc_symbol, pri_date as pri_date, pri_adj_close FROM prices, investments WHERE pri_inv_ticker = inv_ticker AND pri_inv_exc_symbol = inv_exc_symbol AND pri_date between '%s' and '%s' AND pri_adj_close > '%s' and pri_adj_close is not null order by pri_inv_ticker, pri_date" \
                     % (start_date, end_date, MIN_PRICE)
    
    if DEBUG:
        print("get_prices_in_range")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_prices_in_range_for_investment(database, inv_ticker, inv_exc_symbol, start_date, end_date):
    
    # return the price and adjusted close for the investment within the date range
    # only return those whose price is above the minimum to ignore penny stocks
    
    sql_statement =  "SELECT pri_inv_ticker, pri_inv_exc_symbol, pri_date as pri_date, pri_adj_close "
    sql_statement += " FROM prices "
    sql_statement += " WHERE pri_inv_ticker like '%s' " % (inv_ticker)
    sql_statement += " AND pri_inv_exc_symbol like '%s' " % (inv_exc_symbol)
    sql_statement += " AND pri_date between '%s' and '%s' " % (start_date, end_date)
    sql_statement += " AND pri_adj_close > '%s' " % (MIN_PRICE)
    sql_statement += " AND pri_adj_close is not null "
    sql_statement += " ORDER BY pri_date" 
    
    if DEBUG:
        print("get_prices_in_range_for_investment")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_prices_in_range_for_investment_no_limit(database, inv_ticker, inv_exc_symbol, start_date, end_date):
    
    # return the price and adjusted close for the investment within the date range
    # only return those whose price is above the minimum to ignore penny stocks
    
    sql_statement =  "SELECT pri_inv_ticker, pri_inv_exc_symbol, pri_date as pri_date, pri_adj_close "
    sql_statement += " FROM prices "
    sql_statement += " WHERE pri_inv_ticker like '%s' " % (inv_ticker)
    sql_statement += " AND pri_inv_exc_symbol like '%s' " % (inv_exc_symbol)
    sql_statement += " AND pri_date between '%s' and '%s' " % (start_date, end_date)
    sql_statement += " AND pri_adj_close is not null "
    sql_statement += " ORDER BY pri_date" 
    
    if DEBUG:
        print("get_prices_in_range_for_investment")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_prices_in_range_from_drive_table(database, run_name, start_date, end_date):
    
    # return the price and adjusted close for all investment within the date range whose tickers exist in drive table
    
    sql_statement = "SELECT pri_inv_ticker, pri_inv_exc_symbol, pri_date as pri_date, pri_adj_close FROM prices, drive_table WHERE drt_name like '%s' AND pri_inv_ticker = drt_inv_ticker AND pri_inv_exc_symbol = drt_inv_exc_symbol AND pri_date between '%s' and '%s' AND pri_adj_close is not null AND  pri_adj_close > 0 order by pri_date" \
                     % (run_name, start_date, end_date)
    
    if DEBUG:
        print("get_prices_in_range_from_table")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_prices_in_range_for_portfolio(database, portfolio_name, start_date, end_date):
    
    # return the adjusted close for all investments within the date range whose tickers are held in the given PortfolioDB
    
    sql_statement = "SELECT pri_inv_ticker, pri_inv_exc_symbol, pri_date, pri_adj_close "
    sql_statement += " FROM prices, positions p1"
    sql_statement += " WHERE p1.pos_inv_ticker = pri_inv_ticker "
    sql_statement += " AND p1.pos_inv_exc_symbol = pri_inv_exc_symbol "
    sql_statement += " AND p1.pos_date = (SELECT max(p2.pos_date) " 
    sql_statement += " FROM ims_invest.positions p2 "
    sql_statement += " WHERE p1.pos_por_name = p2.pos_por_name " 
    sql_statement += " AND p1.pos_inv_ticker = p2.pos_inv_ticker " 
    sql_statement += " AND p1.pos_inv_exc_symbol = p2.pos_inv_exc_symbol " 
    sql_statement += " AND DATE_FORMAT(p1.pos_date,'%Y-%m') = DATE_FORMAT(p2.pos_date,'%Y-%m') "
    sql_statement += " AND p1.pos_cty_symbol = p2.pos_cty_symbol) " 
    sql_statement += " AND DATE_FORMAT(p1.pos_date,'%Y-%m') = DATE_FORMAT(pri_date,'%Y-%m') "    
    sql_statement += " AND pri_date between '%s' AND '%s' " % (start_date, end_date)
    sql_statement += " AND p1.pos_por_name = '%s'" % (portfolio_name)

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print("get_prices_in_range_for_portfolio")
        print("por_name: %s" % (portfolio_name))    
    
    return result


def get_prices_in_range_for_sector(database, sec_name, start_date, end_date):
    
    # return the price and adjusted close for the investment within the date range
    # only return those whose price is above the minimum to ignore penny stocks
                                      
    sql_statement = "SELECT pri_inv_ticker, pri_date as pri_date, pri_adj_close FROM prices, investments WHERE pri_inv_ticker = inv_ticker AND pri_inv_exc_symbol = inv_exc_symbol AND inv_sec_name like '%s' AND pri_date between '%s' and '%s' AND pri_adj_close > '%s' and pri_adj_close is not null order by pri_date" \
                     % (sec_name, start_date, end_date, MIN_PRICE)
    
    if DEBUG:
        print("get_prices_for_sector for graph")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_stocks_for_portfolio(database, start_date, end_date, min_divs, min_increase, min_price, num_to_return):
    
    sql_statement = "SELECT p1.pri_inv_ticker as inv_ticker, "
    sql_statement += "p1.pri_inv_exc_symbol as inv_exc_symbol, "
    sql_statement += "100*((p2.pri_adj_close-p1.pri_adj_close)/p1.pri_adj_close) as ann_perc_increase, "
    sql_statement += "0 as div_yield "
    sql_statement += "from prices p1, prices p2 "
    sql_statement += "where p1.pri_inv_ticker = p2.pri_inv_ticker "
    sql_statement += "and p1.pri_inv_exc_symbol = p2.pri_inv_exc_symbol "
    sql_statement += "and p1.pri_date <= p2.pri_date "
    sql_statement += "and p1.pri_date >= '%s' " % (start_date)
    sql_statement += "and p1.pri_date <= '%s' " % (end_date)
    sql_statement += "and p1.pri_date = "
    sql_statement += "( "
    sql_statement += "select min(pmin.pri_date) "
    sql_statement += "from prices pmin "
    sql_statement += "where pmin.pri_date >= '%s' " % (start_date)
    sql_statement += "and pmin.pri_date <= '%s' " % (end_date)
    sql_statement += "and pmin.pri_inv_ticker = p1.pri_inv_ticker "
    sql_statement += "and pmin.pri_inv_exc_symbol = p1.pri_inv_exc_symbol "
    sql_statement += ") "
    sql_statement += "and p2.pri_date <= '%s' " % (end_date) 
    sql_statement += "and p2.pri_date >= '%s' " % (start_date) 
    sql_statement += "and p2.pri_date = "
    sql_statement += "( "
    sql_statement += "select max(pmax.pri_date) "
    sql_statement += "from prices pmax "
    sql_statement += "where pmax.pri_date <= '%s' " % (end_date)
    sql_statement += "and pmax.pri_date >= '%s' " % (start_date)
    sql_statement += "and pmax.pri_inv_ticker = p2.pri_inv_ticker "
    sql_statement += "and pmax.pri_inv_exc_symbol = p2.pri_inv_exc_symbol "
    sql_statement += ") "
    sql_statement += "and p1.pri_adj_close > '%s' " % (min_price)
    sql_statement += "and 100*((p2.pri_adj_close-p1.pri_adj_close)/p1.pri_adj_close) > '%s' " % (min_increase)
    sql_statement += "order by 100*((p2.pri_adj_close-p1.pri_adj_close)/p1.pri_adj_close) desc "
    sql_statement += "limit %s" % (num_to_return)

    if DEBUG:
        print("get_stocks_for_portfolio")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_stocks_for_trading(database, start_date, end_date, min_volatility, min_price, min_spread, num_to_return):
    
    sql_statement = "SELECT can_inv_ticker, can_inv_exc_symbol, can_end_datetime, "
    sql_statement += " ((can_last_ask_price-can_last_bid_price)/2) + can_last_bid_price as minpoint, "
    sql_statement += " (can_last_ask_price - can_last_bid_price) as spread "
    sql_statement += " FROM CANDLESTICKS "
    sql_statement += " WHERE can_end_datetime between '%s' AND '%s' " % (start_date, end_date)
    sql_statement += " AND can_last_ask_price > %s " % (min_price)
    sql_statement += " AND (can_last_ask_price - can_last_bid_price) > %s " % (min_spread)
    sql_statement += " ORDER BY can_inv_ticker, can_inv_exc_symbol"
    print(sql_statement)
    
    if DEBUG:
        print("get_stocks_for_trading")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
   
    if DEBUG:
        print(result)
        
    return result


def get_total_improvement_for_portfolio(database, por_name, start_date, end_date):
    
    # return the total increase or decrease in value over time for the PortfolioDB between the start and end dates
    
    sql_statement = "select p1.pos_por_name as por_name, "
    sql_statement += "p1.pos_inv_ticker as inv_ticker, "
    sql_statement += "(p1.pos_value/p1.pos_units) as startprice, "
    sql_statement += "(p2.pos_value/p2.pos_units) as endprice, "
    sql_statement += "sum(dip_div_per_share) as totdiv, "
    sql_statement += "datediff(p2.pos_date, p1.pos_date) as numdays, "
    sql_statement += "100*((p2.pos_value/p2.pos_units) - (p1.pos_value/p1.pos_units) + sum(dip_div_per_share))/(p1.pos_value/p1.pos_units) as percincrease, "
    sql_statement += "(100*((p2.pos_value/p2.pos_units) - (p1.pos_value/p1.pos_units) + sum(dip_div_per_share))/(p1.pos_value/p1.pos_units))/((datediff(p2.pos_date, p1.pos_date))/365) as annualperc "
    sql_statement += "from positions p1, positions p2, dividends_paid "
    sql_statement += "where p1.pos_por_name like '%s' " % (por_name)
    sql_statement += "and p1.pos_por_name = p2.pos_por_name "
    sql_statement += "and p1.pos_inv_ticker = p2.pos_inv_ticker "
    sql_statement += "and p1.pos_inv_exc_symbol = p2.pos_inv_exc_symbol "
    sql_statement += "and p1.pos_date = "
    sql_statement += "( "
    sql_statement += "select min(pmin.pos_date) "
    sql_statement += "from positions pmin "
    sql_statement += "where p1.pos_por_name = pmin.pos_por_name "
    sql_statement += "and p1.pos_inv_ticker = pmin.pos_inv_ticker "
    sql_statement += "and p1.pos_inv_exc_symbol = pmin.pos_inv_exc_symbol "
    sql_statement += "and pmin.pos_date >= '%s' " % (start_date)
    sql_statement += ") "
    sql_statement += "and p2.pos_date =  "
    sql_statement += "( "
    sql_statement += "select max(pmax.pos_date) "
    sql_statement += "from positions pmax "
    sql_statement += "where p2.pos_por_name = pmax.pos_por_name "
    sql_statement += "and p2.pos_inv_ticker = pmax.pos_inv_ticker "
    sql_statement += "and p2.pos_inv_exc_symbol = pmax.pos_inv_exc_symbol "
    sql_statement += "and pmax.pos_date <= '%s' " % (end_date)
    sql_statement += ") "
    sql_statement += "and p1.pos_por_name = dip_por_name "
    sql_statement += "and p1.pos_inv_ticker = DIP_INV_TICKER "
    sql_statement += "and p1.pos_inv_exc_symbol = dip_inv_exc_symbol "
    sql_statement += "and p1.pos_date <= dip_exdiv_date "
    sql_statement += "and p2.pos_date > dip_exdiv_date "
    sql_statement += "and p2.pos_date <= '%s' " % (end_date)
    sql_statement += "and p1.pos_date >= '%s' " % (start_date)
    sql_statement += "group by p1.pos_por_name, p1.pos_inv_ticker, p2.pos_value, p1.pos_value, p2.pos_units, p1.pos_units, p1.pos_date, p2.pos_date "
    sql_statement += "order by p1.pos_por_name, (100*((p2.pos_value/p2.pos_units) - (p1.pos_value/p1.pos_units) + sum(dip_div_per_share))/(p1.pos_value/p1.pos_units))/((datediff(p2.pos_date, p1.pos_date))/365) desc "
    
    if DEBUG:
        print("get_total_improvement_for_portfolio")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_total_improvement_for_portfolio_test(database, test_id):
    
    # return the total increase or decrease in value over time for the PortfolioDB test between the start and end dates
    
    sql_statement = "select pt.ptd_pth_test_id as test_id, "
    sql_statement += "pt.ptd_inv_ticker as inv_ticker, "
    sql_statement += "p1.pri_adj_close as startprice, "
    sql_statement += "p2.pri_adj_close as endprice, "
    sql_statement += "coalesce(100*(sum(div_per_share)/p1.pri_adj_close),0) totdiv, "
    sql_statement += "datediff(p2.pri_date, p1.pri_date) as numdays, "
    sql_statement += "100*(((p2.pri_adj_close - p1.pri_adj_close)/(p1.pri_adj_close)) + coalesce((sum(div_per_share)/p1.pri_adj_close),0)) as percincrease, "
    sql_statement += "(100*(((p2.pri_adj_close - p1.pri_adj_close)/(p1.pri_adj_close)) + coalesce((sum(div_per_share)/p1.pri_adj_close),0)))/((datediff(p2.pri_date, p1.pri_date))/365) as annualperc "
    sql_statement += "from portfolio_test_headers ph "
    sql_statement += "left join portfolio_test_details pt on "
    sql_statement += "(ph.pth_test_id = pt.ptd_pth_test_id) "
    sql_statement += "left join prices p1 on "
    sql_statement += "( p1.pri_inv_ticker = pt.ptd_inv_ticker "
    sql_statement += "and p1.pri_inv_exc_symbol = pt.ptd_inv_exc_symbol "
    sql_statement += "and p1.pri_date >= ph.pth_start_date "
    sql_statement += "and p1.pri_date =  "
    sql_statement += "(  "
    sql_statement += "select min(pmin.pri_date) " 
    sql_statement += "from prices pmin "
    sql_statement += "where p1.pri_inv_ticker = pmin.pri_inv_ticker " 
    sql_statement += "and p1.pri_inv_exc_symbol = pmin.pri_inv_exc_symbol " 
    sql_statement += "and pmin.pri_date >= ph.pth_start_date "
    sql_statement += ") "
    sql_statement += ") "
    sql_statement += "left join prices p2 on "
    sql_statement += "(p2.pri_inv_ticker = pt.ptd_inv_ticker "
    sql_statement += "and p2.pri_inv_exc_symbol = pt.ptd_inv_exc_symbol "
    sql_statement += "and p2.pri_date <= ph.pth_end_date "
    sql_statement += "and p2.pri_date =  "
    sql_statement += "( "
    sql_statement += "select max(pmax.pri_date) "
    sql_statement += "from prices pmax "
    sql_statement += "where p2.pri_inv_ticker = pmax.pri_inv_ticker "
    sql_statement += "and p2.pri_inv_exc_symbol = pmax.pri_inv_exc_symbol "
    sql_statement += "and pmax.pri_date <= ph.pth_end_date "
    sql_statement += ") "
    sql_statement += ") "
    sql_statement += "left outer join dividends di on "
    sql_statement += "(pt.ptd_inv_ticker = di.DIV_INV_TICKER "
    sql_statement += "and pt.ptd_inv_exc_symbol = di.div_inv_exc_symbol  "
    sql_statement += "and p1.pri_date <= di.div_exdiv_date "
    sql_statement += "and p2.pri_date > di.div_exdiv_date "
    sql_statement += ") "
    sql_statement += "where pt.ptd_pth_test_id = '%s' " % (test_id)
    sql_statement += "group by pt.ptd_pth_test_id, pt.ptd_inv_ticker, p2.pri_adj_close, p1.pri_adj_close, p1.pri_date, p2.pri_date "
    sql_statement += "order by 100*(p2.pri_adj_close - p1.pri_adj_close)/(p1.pri_adj_close) desc "

    if DEBUG:
        print("get_total_improvement_for_portfolio_test")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_value_changes_for_portfolio(database, por_name):
    
    # return the value over time for the PortfolioDB (or all portfolios if % passed) 
    # for last year (from today)
    
    sql_statement = "SELECT p1.pos_date as YearMonth, sum(pos_value) as MonthValue "
    sql_statement += "FROM POSITIONS p1 "
    sql_statement += "WHERE p1.pos_por_name like '%s' " % (por_name)
    sql_statement += "AND p1.pos_date >= DATE_SUB(now(), interval 1 year) "
    sql_statement += "GROUP by p1.pos_date  "
    sql_statement += "ORDER by p1.pos_date"
       
    if DEBUG:
        print("get_value_changes_for_portfolio")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_value_over_time_for_portfolio(database, por_name, start_date, end_date, inc_virtual_in_totals):
    
    # return the value over time for the PortfolioDB between the start and end dates
    
    sql_statement = "SELECT p1.pos_por_name as PorName, p1.pos_date as YearMonth, sum(pos_value) as MonthValue "
    sql_statement += "FROM POSITIONS p1, PORTFOLIOS "
    sql_statement += "WHERE p1.pos_por_name like '%s' " % (por_name)
    sql_statement += "AND p1.pos_por_name = por_name "
    sql_statement += "AND p1.pos_date between '%s' and '%s' " % (start_date, end_date)
 
    if inc_virtual_in_totals == 'N':
        sql_statement += " AND por_inc_totals = 'Y' "
    
    sql_statement += "GROUP by p1.pos_por_name, p1.pos_date  "
    sql_statement += "ORDER by p1.pos_por_name, p1.pos_date"
     
    if DEBUG:
        print("get_value_over_time_for_portfolio")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


def get_value_over_time_for_test(database, test_id):
    
    # return the value over time for the PortfolioDB test between the start and end dates
    # held on portfolio_test_header

    sql_statement = "SELECT pri_inv_ticker as Ticker, pri_date as DailyDate, sum(ptd_units*pri_adj_close) as DailyValue "
    sql_statement += "FROM portfolio_test_headers, portfolio_test_details, prices "
    sql_statement += "WHERE pth_test_id like '%s' " % (test_id)
    sql_statement += "AND pth_test_id = ptd_pth_test_id "
    sql_statement += "AND ptd_inv_ticker = pri_inv_ticker "
    sql_statement += "AND ptd_inv_exc_symbol = pri_inv_exc_symbol "
    sql_statement += "AND pri_date between pth_start_date and pth_end_date "
    sql_statement += "GROUP BY pri_inv_ticker, pri_date "
    
    if DEBUG:
        print("get_value_over_time_for_test")

    result = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(result)
        
    return result


